import { useCallback, useContext, useEffect, useRef, type RefCallback } from 'react';
import { animate, useMotionValue, useReducedMotion, type MotionValue } from 'motion/react';
import { FanoutTick } from '../lib/fanout-tick';
import { norm } from '../lib/svg';

/**
 * Glides a MotionValue to each live sample over ~1.6 fanout ticks with ease-out. Callers bind the value to the DOM
 * (see the `useMotion*` helpers) so animation frames do not re-render React — that was the source of stepped motion.
 * Jumps when the user prefers reduced motion.
 */
export function useSmoothedMotion(target: number): MotionValue<number> {
  const tickMs = useContext(FanoutTick);
  const reduceMotion = useReducedMotion();
  const shown = useMotionValue(target);

  useEffect(() => {
    if (reduceMotion) {
      shown.jump(target);
      return;
    }
    const glide = animate(shown, target, {
      duration: (tickMs * 1.6) / 1000,
      ease: [0.22, 1, 0.36, 1],
    });
    return () => glide.stop();
  }, [target, reduceMotion, shown, tickMs]);

  return shown;
}

/**
 * Like `useSmoothedMotion` for a heading in degrees: glides along the shorter way round,
 * so 359° → 1° turns 2° instead of 358°. The result is unwrapped; only use it in `rotate`.
 */
export function useSmoothedAngleMotion(degrees: number): MotionValue<number> {
  const unwrapped = useRef(degrees);
  const delta = ((((degrees - unwrapped.current) % 360) + 540) % 360) - 180;
  const target = unwrapped.current + delta;
  unwrapped.current = target;
  return useSmoothedMotion(target);
}

function useMotionBind<E extends Element>(value: MotionValue<number>, write: (el: E, v: number) => void): RefCallback<E> {
  const elRef = useRef<E | null>(null);
  const writeRef = useRef(write);
  writeRef.current = write;

  useEffect(() => {
    const apply = (v: number) => {
      if (elRef.current) {
        writeRef.current(elRef.current, v);
      }
    };
    apply(value.get());
    return value.on('change', apply);
  }, [value]);

  return useCallback(
    (el: E | null) => {
      elRef.current = el;
      if (el) {
        writeRef.current(el, value.get());
      }
    },
    [value],
  );
}

/** `transform: scaleX/Y` from a sensor value normalized into `[0, 1]`. */
export function useMotionScale(
  target: number,
  min: number,
  max: number,
  axis: 'X' | 'Y',
): RefCallback<HTMLElement> {
  const value = useSmoothedMotion(target);
  return useMotionBind(value, (el, v) => {
    el.style.transform = `scale${axis}(${norm(v, min, max)})`;
  });
}

/** `transform: scaleX` for a zero-centered bar; `v` mapped into `[-1, 1]`. */
export function useMotionCenterScale(target: number, span: number): RefCallback<HTMLElement> {
  const value = useSmoothedMotion(target);
  return useMotionBind(value, (el, v) => {
    const t = Math.max(-1, Math.min(1, v / span));
    el.style.transform = `scaleX(${t})`;
  });
}

/** SVG `transform="rotate(deg cx cy)"` — CSS rotate on `<g>` breaks the origin. */
export function useMotionSvgRotate(
  target: number,
  cx: number,
  cy: number,
  map: (v: number) => number = (v) => v,
): RefCallback<SVGGElement> {
  const value = useSmoothedMotion(target);
  return useMotionBind(value, (el, v) => {
    el.setAttribute('transform', `rotate(${map(v)} ${cx} ${cy})`);
  });
}

/** Same as `useMotionSvgRotate` but with short-way unwrapped degrees. */
export function useMotionSvgAngleRotate(degrees: number, cx: number, cy: number): RefCallback<SVGGElement> {
  const value = useSmoothedAngleMotion(degrees);
  return useMotionBind(value, (el, v) => {
    el.setAttribute('transform', `rotate(${v} ${cx} ${cy})`);
  });
}

/** Rewrites an arc path from a normalized sensor value (0 = `startDeg`, 1 = `startDeg + 2*sweep`). */
export function useMotionSvgArc(
  target: number,
  min: number,
  max: number,
  cx: number,
  cy: number,
  r: number,
  startDeg: number,
  sweep: number,
  describe: (cx: number, cy: number, r: number, start: number, end: number) => string,
): RefCallback<SVGPathElement> {
  const value = useSmoothedMotion(target);
  return useMotionBind(value, (el, v) => {
    const t = norm(v, min, max);
    el.setAttribute('d', describe(cx, cy, r, startDeg, startDeg + 2 * sweep * t));
  });
}

/** Sets `cx` / `cy` from two independent smoothed values. */
export function useMotionSvgDot(
  xTarget: number,
  yTarget: number,
  toX: (x: number) => number,
  toY: (y: number) => number,
): RefCallback<SVGCircleElement> {
  const x = useSmoothedMotion(xTarget);
  const y = useSmoothedMotion(yTarget);
  const elRef = useRef<SVGCircleElement | null>(null);

  useEffect(() => {
    const apply = () => {
      const el = elRef.current;
      if (!el) {
        return;
      }
      el.setAttribute('cx', String(toX(x.get())));
      el.setAttribute('cy', String(toY(y.get())));
    };
    apply();
    const offX = x.on('change', apply);
    const offY = y.on('change', apply);
    return () => {
      offX();
      offY();
    };
  }, [x, y, toX, toY]);

  return useCallback(
    (el: SVGCircleElement | null) => {
      elRef.current = el;
      if (el) {
        el.setAttribute('cx', String(toX(x.get())));
        el.setAttribute('cy', String(toY(y.get())));
      }
    },
    [x, y, toX, toY],
  );
}
