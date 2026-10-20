export function clamp01(t: number): number {
  return Math.min(1, Math.max(0, t));
}

export function norm(value: number, min: number, max: number): number {
  return max === min ? 0 : clamp01((value - min) / (max - min));
}

/** Point on a circle; `deg` uses compass angles (0 = up, clockwise). */
export function polar(cx: number, cy: number, r: number, deg: number): { x: number; y: number } {
  const rad = ((deg - 90) * Math.PI) / 180;
  return { x: cx + r * Math.cos(rad), y: cy + r * Math.sin(rad) };
}

/** SVG arc path between two compass angles. */
export function describeArc(cx: number, cy: number, r: number, startDeg: number, endDeg: number): string {
  if (Math.abs(endDeg - startDeg) < 0.05) {
    const p = polar(cx, cy, r, startDeg);
    return `M ${p.x} ${p.y}`;
  }
  const start = polar(cx, cy, r, startDeg);
  const end = polar(cx, cy, r, endDeg);
  const large = Math.abs(endDeg - startDeg) > 180 ? 1 : 0;
  const sweep = endDeg >= startDeg ? 1 : 0;
  return `M ${start.x} ${start.y} A ${r} ${r} 0 ${large} ${sweep} ${end.x} ${end.y}`;
}
