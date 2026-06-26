package talkbackswitch

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.viam.com/rdk/components/audioin"
	"go.viam.com/rdk/components/audioout"
	toggleswitch "go.viam.com/rdk/components/switch"
	"go.viam.com/rdk/logging"
	"go.viam.com/rdk/resource"
	rutils "go.viam.com/rdk/utils"
)

// AudioSwitch is a switch component that, in the "record" position, streams live
// audio from a configured audio_in (microphone) straight to a configured
// audio_out (speaker). It is a live monitor/intercom -- nothing is saved to disk.
var TalkbackSwitch = resource.NewModel("mattmacf", "talkback-switch", "talkback-switch")

const (
	posOff    uint32 = 0
	posRecord uint32 = 1
	numPos    uint32 = 2

	defaultChunkSec      float32       = 1.0
	getAudioRetryBackoff time.Duration = 500 * time.Millisecond
	streamReopenBackoff  time.Duration = 250 * time.Millisecond
	playTimeout          time.Duration = 5 * time.Second
)

func init() {
	resource.RegisterComponent(toggleswitch.API, TalkbackSwitch,
		resource.Registration[toggleswitch.Switch, *TalkbackSwitchConfig]{
			Constructor: NewTalkbackSwitch,
		},
	)
}

// AudioSwitchConfig configures an audio passthrough switch.
type TalkbackSwitchConfig struct {
	// AudioInput is the name of the audio_in (microphone) component to read from.
	AudioInput string `json:"audio_input"`
	// AudioOutput is the name of the audio_out (speaker) component to play to.
	AudioOutput string `json:"audio_output"`
	// Codec optionally overrides codec negotiation. When empty, a common codec is
	// chosen by intersecting the mic and speaker SupportedCodecs.
	Codec string `json:"codec,omitempty"`
	// ChunkDurationSeconds is the duration requested per GetAudio call. Defaults to
	// defaultChunkSec when unset. Smaller values reduce latency.
	ChunkDurationSeconds float32 `json:"chunk_duration_seconds,omitempty"`
}

// Validate ensures both audio dependencies are configured and returns them as
// required dependencies.
func (cfg *TalkbackSwitchConfig) Validate(path string) ([]string, []string, error) {
	if cfg.AudioInput == "" {
		return nil, nil, fmt.Errorf("%s: audio_input is required", path)
	}
	if cfg.AudioOutput == "" {
		return nil, nil, fmt.Errorf("%s: audio_output is required", path)
	}
	if cfg.ChunkDurationSeconds < 0 {
		return nil, nil, fmt.Errorf("%s: chunk_duration_seconds must be >= 0", path)
	}
	return []string{cfg.AudioInput, cfg.AudioOutput}, nil, nil
}

type talkbackSwitch struct {
	resource.AlwaysRebuild

	name   resource.Name
	logger logging.Logger
	cfg    *TalkbackSwitchConfig

	audioIn  audioin.AudioIn
	audioOut audioout.AudioOut

	codec        string            // negotiated codec spelling (as advertised by the mic)
	fallbackInfo *rutils.AudioInfo // used when a chunk carries no AudioInfo
	durationSec  float32

	cancelCtx  context.Context // component lifetime; cancelled in Close
	cancelFunc func()

	mu           sync.Mutex
	position     uint32
	streamCancel context.CancelFunc // non-nil iff a passthrough goroutine is running
	streamDone   chan struct{}      // closed by the goroutine when it exits
}

func NewTalkbackSwitch(
	ctx context.Context, deps resource.Dependencies, rawConf resource.Config, logger logging.Logger,
) (toggleswitch.Switch, error) {
	conf, err := resource.NativeConfig[*TalkbackSwitchConfig](rawConf)
	if err != nil {
		return nil, err
	}

	cancelCtx, cancelFunc := context.WithCancel(context.Background())

	audioIn, err := audioin.FromProvider(deps, conf.AudioInput)
	if err != nil {
		cancelFunc()
		return nil, fmt.Errorf("failed to get audio_in %q: %w", conf.AudioInput, err)
	}
	audioOut, err := audioout.FromProvider(deps, conf.AudioOutput)
	if err != nil {
		cancelFunc()
		return nil, fmt.Errorf("failed to get audio_out %q: %w", conf.AudioOutput, err)
	}

	dur := conf.ChunkDurationSeconds
	if dur == 0 {
		dur = defaultChunkSec
	}

	s := &talkbackSwitch{
		name:        rawConf.ResourceName(),
		logger:      logger,
		cfg:         conf,
		audioIn:     audioIn,
		audioOut:    audioOut,
		durationSec: dur,
		position:    posOff,
		cancelCtx:   cancelCtx,
		cancelFunc:  cancelFunc,
	}

	// Best-effort negotiation now; retried lazily by the passthrough loop if the
	// audio components are not yet reachable at construction time.
	s.negotiate(ctx)

	return s, nil
}

func (s *talkbackSwitch) Name() resource.Name {
	return s.name
}

// GetNumberOfPositions reports the two positions: 0 = off, 1 = record.
func (s *talkbackSwitch) GetNumberOfPositions(ctx context.Context, extra map[string]interface{}) (uint32, []string, error) {
	return numPos, []string{"off", "record"}, nil
}

func (s *talkbackSwitch) GetPosition(ctx context.Context, extra map[string]interface{}) (uint32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.position, nil
}

// SetPosition starts (position 1) or stops (position 0) live audio passthrough.
func (s *talkbackSwitch) SetPosition(ctx context.Context, position uint32, extra map[string]interface{}) error {
	if position >= numPos {
		return toggleswitch.ErrInvalidPosition(s.name.Name, int(position), int(numPos-1))
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if position == s.position {
		return nil // idempotent
	}
	if position == posRecord {
		s.startStreamLocked()
	} else {
		s.stopStreamLocked()
	}
	s.position = position
	return nil
}

// startStreamLocked launches the passthrough goroutine. Must hold s.mu.
func (s *talkbackSwitch) startStreamLocked() {
	if s.streamCancel != nil {
		return // already running
	}
	streamCtx, cancel := context.WithCancel(s.cancelCtx)
	done := make(chan struct{})
	s.streamCancel = cancel
	s.streamDone = done
	s.logger.Info("audio-switch: starting live audio passthrough")
	go s.runPassthrough(streamCtx, done)
}

// stopStreamLocked cancels and joins the passthrough goroutine, then flushes any
// in-flight playback. Must hold s.mu (the join is bounded so holding the lock is fine).
func (s *talkbackSwitch) stopStreamLocked() {
	if s.streamCancel == nil {
		return // already stopped
	}
	s.streamCancel()
	done := s.streamDone
	s.streamCancel = nil
	s.streamDone = nil
	<-done
	s.logger.Info("audio-switch: stopped live audio passthrough")

	// Best-effort: tell the speaker to flush whatever is still queued. Use a
	// fresh short-lived context derived from the component lifetime.
	flushCtx, cancel := context.WithTimeout(s.cancelCtx, 2*time.Second)
	defer cancel()
	if _, err := s.audioOut.DoCommand(flushCtx, map[string]interface{}{"command": "stop"}); err != nil {
		s.logger.Debugf("audio-switch: audio_out stop command failed (non-fatal): %v", err)
	}
}

// runPassthrough reads audio chunks from the mic and plays them on the speaker
// until ctx is cancelled. It re-opens the stream if it ends while still recording,
// so it works whether GetAudio yields a bounded or continuous stream.
func (s *talkbackSwitch) runPassthrough(ctx context.Context, done chan struct{}) {
	defer close(done)

	for {
		if ctx.Err() != nil {
			return
		}
		if s.codec == "" {
			s.negotiate(ctx)
			if s.codec == "" {
				s.logger.Warn("audio-switch: no usable codec yet; retrying")
				if !sleepCtx(ctx, getAudioRetryBackoff) {
					return
				}
				continue
			}
		}

		// GetAudio blocks until the first chunk arrives or errors. previousTimestampNs=0
		// requests live audio (no historical resume point).
		ch, err := s.audioIn.GetAudio(ctx, s.codec, s.durationSec, 0, nil)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.logger.Warnf("audio-switch: GetAudio failed, retrying: %v", err)
			if !sleepCtx(ctx, getAudioRetryBackoff) {
				return
			}
			continue
		}

		s.pumpChunks(ctx, ch)

		// Stream ended (EOF/error/cancel). Re-open if still recording.
		if ctx.Err() != nil {
			return
		}
		if !sleepCtx(ctx, streamReopenBackoff) {
			return
		}
	}
}

// pumpChunks plays every chunk from ch until the channel closes or ctx is cancelled.
func (s *talkbackSwitch) pumpChunks(ctx context.Context, ch chan *audioin.AudioChunk) {
	for {
		select {
		case <-ctx.Done():
			return
		case chunk, ok := <-ch:
			if !ok {
				return // channel closed by the audioin client
			}
			if len(chunk.AudioData) == 0 {
				continue
			}
			info := chunk.AudioInfo
			if info == nil {
				info = s.fallbackInfo
			}
			// Bound each Play so a wedged speaker cannot deadlock stop/Close.
			playCtx, cancel := context.WithTimeout(ctx, playTimeout)
			err := s.audioOut.Play(playCtx, chunk.AudioData, info, nil)
			cancel()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				s.logger.Warnf("audio-switch: Play failed (continuing): %v", err)
			}
		}
	}
}

// negotiate chooses a codec common to the mic and speaker (config override wins)
// and caches a fallback AudioInfo. Best-effort: logs and returns on failure.
func (s *talkbackSwitch) negotiate(ctx context.Context) {
	inProps, err := s.audioIn.Properties(ctx, nil)
	if err != nil {
		s.logger.Warnf("audio-switch: audio_in Properties failed: %v", err)
		return
	}
	outProps, err := s.audioOut.Properties(ctx, nil)
	if err != nil {
		s.logger.Warnf("audio-switch: audio_out Properties failed: %v", err)
		return
	}

	chosen := s.cfg.Codec
	if chosen == "" {
		chosen = chooseCodec(inProps.SupportedCodecs, outProps.SupportedCodecs)
	}
	if chosen == "" {
		s.logger.Warnf("audio-switch: no common codec between audio_in=%v and audio_out=%v",
			inProps.SupportedCodecs, outProps.SupportedCodecs)
		return
	}

	s.codec = chosen
	s.fallbackInfo = &rutils.AudioInfo{
		Codec:        chosen,
		SampleRateHz: inProps.SampleRateHz,
		NumChannels:  inProps.NumChannels,
	}
	s.logger.Infof("audio-switch: using codec %q (%d Hz, %d ch)", chosen, inProps.SampleRateHz, inProps.NumChannels)

	if inProps.SampleRateHz != outProps.SampleRateHz || inProps.NumChannels != outProps.NumChannels {
		s.logger.Warnf(
			"audio-switch: mic/speaker format mismatch (in %d Hz/%d ch, out %d Hz/%d ch); passthrough may distort",
			inProps.SampleRateHz, inProps.NumChannels, outProps.SampleRateHz, outProps.NumChannels)
	}
}

func (s *talkbackSwitch) DoCommand(ctx context.Context, cmd map[string]interface{}) (map[string]interface{}, error) {
	command, _ := cmd["command"].(string)
	switch command {
	case "get_state":
		s.mu.Lock()
		pos := s.position
		recording := s.streamCancel != nil
		s.mu.Unlock()
		return map[string]interface{}{
			"position":  pos,
			"recording": recording,
			"codec":     s.codec,
		}, nil
	default:
		return nil, errors.New("command not implemented")
	}
}

func (s *talkbackSwitch) Status(ctx context.Context) (map[string]interface{}, error) {
	s.mu.Lock()
	pos := s.position
	recording := s.streamCancel != nil
	s.mu.Unlock()
	return map[string]interface{}{
		"position":  pos,
		"recording": recording,
		"codec":     s.codec,
	}, nil
}

func (s *talkbackSwitch) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.streamCancel != nil {
		s.stopStreamLocked()
	}
	s.position = posOff
	s.mu.Unlock()

	s.cancelFunc()
	return nil
}

// chooseCodec returns the mic's spelling of a codec supported by both the mic and
// speaker. Matching is case/underscore-insensitive (e.g. "PCM_16" == "pcm16"),
// preferring uncompressed PCM. Returns "" when there is no common codec.
func chooseCodec(inCodecs, outCodecs []string) string {
	inByNorm := make(map[string]string, len(inCodecs))
	for _, c := range inCodecs {
		inByNorm[normalizeCodec(c)] = c
	}
	outSet := make(map[string]bool, len(outCodecs))
	for _, c := range outCodecs {
		outSet[normalizeCodec(c)] = true
	}

	for _, pref := range []string{"pcm16", "pcm32", "pcm32float"} {
		if in, ok := inByNorm[pref]; ok && outSet[pref] {
			return in
		}
	}
	for _, c := range inCodecs {
		if outSet[normalizeCodec(c)] {
			return c
		}
	}
	return ""
}

func normalizeCodec(c string) string {
	return strings.ToLower(strings.ReplaceAll(c, "_", ""))
}

// sleepCtx waits for d or until ctx is cancelled. Returns false if ctx was cancelled.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
