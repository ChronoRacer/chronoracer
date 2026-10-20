import { useEffect, useRef, useState } from 'react';
import { useMotionValueEvent, useReducedMotion, useSpring } from 'motion/react';

/** Critically-ish damped — tracks samples without overshoot that blows past gauge ends. */
const SPRING = { stiffness: 320, damping: 36, mass: 0.4, restDelta: 0.01 } as const;

/**
 * Smooths a live sensor sample with Motion's spring. Jumps when the user prefers
 * reduced motion so the readout stays exact.
 */
export function useSmoothedNumber(target: number): number {
  const reduceMotion = useReducedMotion();
  const spring = useSpring(target, SPRING);
  const [value, setValue] = useState(target);

  useEffect(() => {
    if (reduceMotion) {
      spring.jump(target);
    } else {
      spring.set(target);
    }
  }, [target, reduceMotion, spring]);

  useMotionValueEvent(spring, 'change', setValue);
  return value;
}

/**
 * Like `useSmoothedNumber` for a heading in degrees: springs along the shorter way round,
 * so 359° → 1° turns 2° instead of 358°. The result is unwrapped; only use it in `rotate`.
 */
export function useSmoothedAngle(degrees: number): number {
  const unwrapped = useRef(degrees);
  const delta = ((((degrees - unwrapped.current) % 360) + 540) % 360) - 180;
  const target = unwrapped.current + delta;
  unwrapped.current = target;
  return useSmoothedNumber(target);
}
