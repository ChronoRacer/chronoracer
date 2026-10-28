import { useCallback, useContext, useEffect, useRef, useSyncExternalStore } from 'react';
import { animate, useMotionValue, useReducedMotion } from 'motion/react';
import { FanoutTick } from '../lib/fanout-tick';

/**
 * Glides to a live sensor sample linearly over one fanout tick: the widget reaches each sample as the next one
 * arrives, so it moves at a steady speed and never overshoots the gauge ends. Jumps when the user prefers reduced
 * motion so the readout stays exact.
 *
 * The value is read as an external store so React renders it in the same frame Motion computes it. A state update
 * would render after the frame, one frame late except when a socket message renders sooner, and that uneven lag
 * shows as a hitch on every push.
 */
export function useSmoothedNumber(target: number): number {
  const tickMs = useContext(FanoutTick);
  const reduceMotion = useReducedMotion();
  const shown = useMotionValue(target);
  const subscribe = useCallback((onChange: () => void) => shown.on('change', onChange), [shown]);
  const value = useSyncExternalStore(subscribe, () => shown.get());

  useEffect(() => {
    if (reduceMotion) {
      shown.jump(target);
      return;
    }
    const glide = animate(shown, target, { duration: tickMs / 1000, ease: 'linear' });
    return () => glide.stop();
  }, [target, reduceMotion, shown, tickMs]);

  return value;
}

/**
 * Like `useSmoothedNumber` for a heading in degrees: glides along the shorter way round,
 * so 359° → 1° turns 2° instead of 358°. The result is unwrapped; only use it in `rotate`.
 */
export function useSmoothedAngle(degrees: number): number {
  const unwrapped = useRef(degrees);
  const delta = ((((degrees - unwrapped.current) % 360) + 540) % 360) - 180;
  const target = unwrapped.current + delta;
  unwrapped.current = target;
  return useSmoothedNumber(target);
}
