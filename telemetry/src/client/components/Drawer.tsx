import type { ReactNode } from 'react';

type Props = {
  open: boolean;
  title: string;
  onClose: () => void;
  children: ReactNode;
};

/** Full-stage overlay for detail views. Page itself never scrolls; the drawer body may. */
export function Drawer({ open, title, onClose, children }: Props) {
  if (!open) {
    return null;
  }
  return (
    <div className="drawer" role="dialog" aria-modal="true" aria-label={title}>
      <header className="drawer-head">
        <h2 className="zone-title">{title}</h2>
        <button type="button" className="drawer-close" onClick={onClose}>
          CLOSE
        </button>
      </header>
      <div className="drawer-body">{children}</div>
    </div>
  );
}
