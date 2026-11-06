import { t, useLocale } from '../shared/i18n.js';
import { useEffect, useMemo, useRef, useState, type KeyboardEvent as ReactKeyboardEvent } from 'react';
import { filterProductCommands, type ProductCommand } from './commandPaletteModel.js';

function focusElement(id: string | undefined) {
  if (!id) return;
  const element = document.getElementById(id);
  if (element instanceof HTMLElement) {
    if (!element.matches('a,button,input,select,textarea,[tabindex]')) element.tabIndex = -1;
    element.focus({ preventScroll: true });
  }
}

function runCommand(command: ProductCommand) {
  if (command.availability !== 'available') return;
  const target = document.getElementById(command.targetId);
  target?.scrollIntoView({ block: 'start', behavior: 'smooth' });
  window.history.replaceState(null, '', `#${command.targetId}`);
  window.setTimeout(() => focusElement(command.focusId ?? command.targetId), 0);
}

export function CommandPalette() {
  const locale = useLocale();
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState('');
  const [activeIndex, setActiveIndex] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);
  const paletteRef = useRef<HTMLElement>(null);
  const previousFocusRef = useRef<HTMLElement | null>(null);
  const commands = useMemo(() => filterProductCommands(query), [query, locale]);

  const close = (restoreFocus = true) => {
    setOpen(false);
    setQuery('');
    setActiveIndex(0);
    if (restoreFocus) window.setTimeout(() => previousFocusRef.current?.focus({ preventScroll: true }), 0);
  };

  const activate = (command: ProductCommand) => {
    if (command.availability !== 'available') return;
    close(false);
    runCommand(command);
  };

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      const modifier = event.ctrlKey || event.metaKey;
      if (modifier && event.key.toLocaleLowerCase() === 'k') {
        event.preventDefault();
        if (open) {
          close();
        } else {
          previousFocusRef.current = document.activeElement instanceof HTMLElement ? document.activeElement : null;
          setOpen(true);
        }
        return;
      }

      if (!open) return;
      if (event.key === 'Escape') {
        event.preventDefault();
        close();
      }
    };

    document.addEventListener('keydown', onKeyDown);
    return () => document.removeEventListener('keydown', onKeyDown);
  }, [open]);

  useEffect(() => {
    if (!open || !paletteRef.current) return;
    // Inert all sibling branches, including toolbar controls, without hiding
    // the dialog's ancestors from assistive technology.
    const previous: {element: HTMLElement; inert: boolean}[] = [];
    let branch: HTMLElement = paletteRef.current;
    while (branch.parentElement && branch.parentElement !== document.body) {
      for (const sibling of Array.from(branch.parentElement.children)) {
        if (sibling !== branch && sibling instanceof HTMLElement) { previous.push({element: sibling, inert: sibling.inert}); sibling.inert = true; }
      }
      branch = branch.parentElement;
    }
    const onFocus = (event: FocusEvent) => { if (event.target instanceof Node && !paletteRef.current?.contains(event.target)) inputRef.current?.focus(); };
    document.addEventListener('focusin', onFocus);
    return () => { previous.forEach(item => { item.element.inert = item.inert; }); document.removeEventListener('focusin', onFocus); };
  }, [open]);

  useEffect(() => {
    if (!open) return;
    setActiveIndex(0);
    window.setTimeout(() => inputRef.current?.focus(), 0);
  }, [open]);

  useEffect(() => {
    if (activeIndex >= commands.length) setActiveIndex(Math.max(0, commands.length - 1));
  }, [activeIndex, commands.length]);

  const onDialogKeyDown = (event: ReactKeyboardEvent<HTMLElement>) => {
    if (event.key !== 'Tab') return;
    const palette = paletteRef.current;
    if (!palette) return;
    const focusable = Array.from(
      palette.querySelectorAll<HTMLElement>(
        'button:not(:disabled), input:not(:disabled), [href], [tabindex]:not([tabindex="-1"])',
      ),
    ).filter(element => !element.hasAttribute('hidden'));

    if (focusable.length === 0) {
      event.preventDefault();
      return;
    }

    const first = focusable[0];
    const last = focusable[focusable.length - 1];
    if (event.shiftKey && document.activeElement === first) {
      event.preventDefault();
      last?.focus();
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault();
      first?.focus();
    }
  };

  const onInputKeyDown = (event: ReactKeyboardEvent<HTMLInputElement>) => {
    if (event.key === 'ArrowDown') {
      event.preventDefault();
      if (commands.length > 0) setActiveIndex(index => (index + 1) % commands.length);
      return;
    }
    if (event.key === 'ArrowUp') {
      event.preventDefault();
      if (commands.length > 0) setActiveIndex(index => (index - 1 + commands.length) % commands.length);
      return;
    }
    if (event.key === 'Enter') {
      event.preventDefault();
      const command = commands[activeIndex];
      if (command) activate(command);
    }
  };

  return <>
    <button
      className="btn btn-sm btn-outline-secondary kafdeck-command-trigger"
      type="button"
      onClick={() => {
        previousFocusRef.current = document.activeElement instanceof HTMLElement ? document.activeElement : null;
        setOpen(true);
      }}
      aria-haspopup="dialog"
      aria-expanded={open}
      aria-controls="kafdeck-command-palette"
    >
      {t("Commands")}{' '}<kbd>Ctrl K</kbd>
    </button>
    {open && <div className="kafdeck-command-backdrop" role="presentation" onMouseDown={event => {
      if (event.currentTarget === event.target) close();
    }}>
      <section
        ref={paletteRef}
        id="kafdeck-command-palette"
        className="card kafdeck-command-palette"
        role="dialog"
        onKeyDown={onDialogKeyDown}
        aria-modal="true"
        aria-labelledby="kafdeck-command-title"
      >
        <div className="card-header">
          <div>
            <h2 id="kafdeck-command-title" className="card-title">{t("Command palette")}</h2>
            <div className="text-secondary">{t("Keyboard-first navigation over registered Kafdeck product surfaces.")}</div>
          </div>
          <button className="btn-close ms-auto" type="button" aria-label={t("Close command palette")} onClick={() => close()} />
        </div>
        <div className="card-body">
          <label className="form-label" htmlFor="kafdeck-command-query">{t("Find a resource or tool")}</label>
          <input dir="ltr"
            ref={inputRef}
            id="kafdeck-command-query"
            className="form-control"
            type="search"
            role="combobox"
            aria-controls="kafdeck-command-results"
            aria-autocomplete="list"
            aria-expanded="true"
            aria-activedescendant={commands[activeIndex] ? `kafdeck-command-${commands[activeIndex].id}` : undefined}
            autoComplete="off"
            value={query}
            onChange={event => setQuery(event.target.value)}
            onKeyDown={onInputKeyDown}
            placeholder={t("Topics, schemas, Connect, ksqlDB, data jobs…")}
          />
        </div>
        <div id="kafdeck-command-results" className="list-group list-group-flush" role="listbox" aria-label={t("Kafdeck commands")}>
          {commands.length === 0 && <div className="list-group-item text-secondary" role="status">{t("No registered command matches this search.")}</div>}
          {commands.map((command, index) => {
            const unavailable = command.availability !== 'available';
            return <button
              id={`kafdeck-command-${command.id}`}
              key={command.id}
              className={`list-group-item list-group-item-action kafdeck-command-item${index === activeIndex ? ' active' : ''}`}
              type="button"
              role="option"
              aria-selected={index === activeIndex}
              aria-disabled={unavailable}
              disabled={unavailable}
              onMouseEnter={() => setActiveIndex(index)}
              onClick={() => activate(command)}
            >
              <span>
                <strong>{t(command.label)}</strong>
                <small>{t(command.description)}</small>
              </span>
              {unavailable && <span className="badge bg-yellow-lt text-yellow">{t("Unavailable")}</span>}
            </button>;
          })}
        </div>
        <div className="card-footer text-secondary">
          {t("↑/↓ move · Enter open · Esc close. Unavailable commands stay visible instead of falling through to an untyped shortcut.")}{' '}</div>
      </section>
    </div>}
  </>;
}
