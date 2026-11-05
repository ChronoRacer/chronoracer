import { useContext, useEffect, useRef, useState } from 'react';
import L from 'leaflet';
import 'leaflet/dist/leaflet.css';
import { animate, useMotionValue, useReducedMotion } from 'motion/react';
import { useSensor } from '../hooks/useSensor';
import { useSmoothedAngleMotion } from '../hooks/useSmoothedNumber';
import { FanoutTick } from '../lib/fanout-tick';
import { cardinal, formatFixed } from '../lib/format';
import type { SeriesStore } from '../lib/series-store';
import { trailOf } from '../lib/trail';
import { cx, describe, liveClass } from './instruments';

/** GUGiK orthophoto (open data, Poland only) on the Web Mercator tile grid. */
const ORTHO_URL =
  'https://mapy.geoportal.gov.pl/wss/service/PZGIK/ORTO/WMTS/StandardResolution?SERVICE=WMTS&REQUEST=GetTile' +
  '&VERSION=1.0.0&LAYER=ORTOFOTOMAPA&TILEMATRIXSET=EPSG:3857&TILEMATRIX=EPSG:3857:{z}&TILEROW={y}&TILECOL={x}';
const ORTHO_ATTRIBUTION = 'Orthophoto © <a href="https://www.geoportal.gov.pl/" target="_blank" rel="noreferrer">GUGiK</a>';
const ORTHO_MAX_ZOOM = 19;
const POLAND: L.LatLngTuple = [52.1, 19.4];
const FOLLOW_ZOOM = 17;

type MapParts = { map: L.Map; trail: L.Polyline; car: L.Marker; arrow: HTMLElement };

/** Orthophoto map: car position and heading, and the trail driven since the page opened. */
export function GpsTrack({ store }: { store: SeriesStore }) {
  const lat = useSensor(store, 'gps_lat');
  const lon = useSensor(store, 'gps_lon');
  const heading = useSensor(store, 'gps_heading');
  const fix = useSensor(store, 'gps_fix');
  const tickMs = useContext(FanoutTick);
  const reduceMotion = useReducedMotion();
  const box = useRef<HTMLDivElement>(null);
  const [parts, setParts] = useState<MapParts>();
  const [follow, setFollow] = useState(true);
  const following = useRef(follow);
  following.current = follow;
  const shownLat = useMotionValue(Number.NaN);
  const shownLon = useMotionValue(Number.NaN);
  const angle = useSmoothedAngleMotion(heading.finite ?? 0);

  useEffect(() => {
    const el = box.current;
    if (!el) {
      return;
    }
    // Zoom keeps the car in the middle; dragging hands the view to the user until FOLLOW CAR.
    const map = L.map(el, {
      center: POLAND,
      zoom: 6,
      zoomControl: false,
      attributionControl: false,
      scrollWheelZoom: 'center',
      doubleClickZoom: 'center',
      touchZoom: 'center',
    });
    L.control.zoom({ position: 'topright' }).addTo(map);
    L.control.attribution({ position: 'bottomright', prefix: false }).addAttribution(ORTHO_ATTRIBUTION).addTo(map);
    L.tileLayer(ORTHO_URL, { maxNativeZoom: ORTHO_MAX_ZOOM, maxZoom: ORTHO_MAX_ZOOM + 1, minZoom: 6 }).addTo(map);
    const trail = L.polyline([], { className: 'map-trail', interactive: false }).addTo(map);
    const arrow = document.createElement('div');
    arrow.className = 'car-arrow';
    arrow.innerHTML = '<svg viewBox="0 0 32 32"><path d="M16 3 L27 28 L16 21 L5 28 Z" /></svg>';
    const car = L.marker(POLAND, {
      icon: L.divIcon({ className: 'car-marker', html: arrow, iconSize: [32, 32] }),
      interactive: false,
      keyboard: false,
    });
    map.on('dragstart', () => setFollow(false));
    const resize = new ResizeObserver(() => map.invalidateSize());
    resize.observe(el);
    setParts({ map, trail, car, arrow });
    return () => {
      resize.disconnect();
      map.remove();
    };
  }, []);

  // Trail: append new points; redraw only after old ones were trimmed.
  useEffect(() => {
    if (!parts) {
      return;
    }
    const trail = trailOf(store);
    let epoch = -1;
    let drawn = 0;
    const draw = () => {
      if (epoch !== trail.epoch) {
        epoch = trail.epoch;
        drawn = trail.points.length;
        parts.trail.setLatLngs(trail.points);
        return;
      }
      for (; drawn < trail.points.length; drawn++) {
        parts.trail.addLatLng(trail.points[drawn]);
      }
    };
    draw();
    return trail.subscribe(draw);
  }, [parts, store]);

  // Car position glides like the other instruments; the first fix jumps there.
  useEffect(() => {
    if (lat.finite === undefined || lon.finite === undefined) {
      return;
    }
    if (reduceMotion || !Number.isFinite(shownLat.get())) {
      shownLat.jump(lat.finite);
      shownLon.jump(lon.finite);
      return;
    }
    const glide = { duration: (tickMs * 1.6) / 1000, ease: [0.22, 1, 0.36, 1] as const };
    const toLat = animate(shownLat, lat.finite, glide);
    const toLon = animate(shownLon, lon.finite, glide);
    return () => {
      toLat.stop();
      toLon.stop();
    };
  }, [lat.finite, lon.finite, reduceMotion, shownLat, shownLon, tickMs]);

  useEffect(() => {
    if (!parts) {
      return;
    }
    const { map, car } = parts;
    let frame = 0;
    let zooming = false;
    const place = () => {
      frame = 0;
      const at = shownLat.get();
      const to = shownLon.get();
      if (!Number.isFinite(at) || !Number.isFinite(to)) {
        return;
      }
      car.setLatLng([at, to]);
      if (!map.hasLayer(car)) {
        car.addTo(map);
        map.setView([at, to], FOLLOW_ZOOM, { animate: false });
      } else if (following.current && !zooming) {
        map.setView([at, to], map.getZoom(), { animate: false });
      }
    };
    const schedule = () => {
      frame ||= requestAnimationFrame(place);
    };
    const onZoomStart = () => {
      zooming = true;
    };
    const onZoomEnd = () => {
      zooming = false;
    };
    map.on('zoomstart', onZoomStart).on('zoomend', onZoomEnd);
    schedule();
    const offLat = shownLat.on('change', schedule);
    const offLon = shownLon.on('change', schedule);
    return () => {
      cancelAnimationFrame(frame);
      offLat();
      offLon();
      map.off('zoomstart', onZoomStart).off('zoomend', onZoomEnd);
    };
  }, [parts, shownLat, shownLon]);

  useEffect(() => {
    if (!parts) {
      return;
    }
    const turn = (deg: number) => {
      parts.arrow.style.transform = `rotate(${deg}deg)`;
    };
    turn(angle.get());
    return angle.on('change', turn);
  }, [parts, angle]);

  const recenter = () => {
    setFollow(true);
    const at = shownLat.get();
    const to = shownLon.get();
    if (parts && Number.isFinite(at) && Number.isFinite(to)) {
      parts.map.setView([at, to], Math.max(parts.map.getZoom(), FOLLOW_ZOOM - 1));
    }
  };

  const located = Number.isFinite(shownLat.get()) || (lat.finite !== undefined && lon.finite !== undefined);
  const lost = lat.stale || lon.stale || fix.finite === undefined || fix.finite < 2;

  return (
    <section className={cx('card card-map area-map', lost && 'no-fix')} aria-label="Track map">
      {/* Static className: Leaflet adds its own classes to this element. */}
      <div ref={box} className="map" />
      <div className="map-overlay map-top">
        <span className="map-title">TRACK</span>
        {located && lost ? (
          <span className="chip tone-bad" title={describe(fix, lat, lon)}>
            NO GPS FIX · LAST POSITION
          </span>
        ) : located && fix.finite === 2 ? (
          <span className="chip tone-warn" title={describe(fix)}>
            GPS 2D
          </span>
        ) : null}
      </div>
      <div className="map-overlay map-bottom">
        <span className={cx('map-heading', liveClass(heading))} title={describe(heading)}>
          HDG <span className="readout-value">{formatFixed(heading.value, 0)}°</span>
          {heading.finite === undefined ? '' : ` ${cardinal(heading.finite)}`}
        </span>
        {follow ? null : (
          <button type="button" className="map-button" onClick={recenter}>
            FOLLOW CAR
          </button>
        )}
      </div>
      {located ? null : <div className="map-wait">WAITING FOR GPS FIX</div>}
    </section>
  );
}
