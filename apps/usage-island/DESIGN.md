# AI Usage Island Design

## Scene and theme

A developer glances away from a terminal to a small top-centre usage summary on an external display, then returns to work. The dark content surface deliberately continues the existing charcoal status strip and can blend with a MacBook's physical notch; it is not a generic dashboard theme.

## Color strategy

Restrained. Inherit the existing status-strip palette: text #E3E7ED, secondary captions #8EACCC, positive quota #A4D2B7, warning #E7BD7B, error #F39A96 and subtle separators #2A3038. Notch-style chrome on every display remains managed by DynamicNotchKit. Color always accompanies a written status or labelled number.

## Typography

Use installed JetBrains Mono Regular for values at 13pt, matching the existing strip and Rex. Use native system type for section headings and controls. Values have monospaced digits, stable widths and no animated number transitions. Expanded title is 16pt semibold. Captions are 12pt.

## Layout

Collapsed on every display: separate leading Today and trailing Week left summaries in a notch-style surface attached to the top edge, at menu-bar height. A warning indicator appears if data is stale or unavailable. Preserve the hardware cutout on a notched screen; on other screens use DynamicNotchKit's virtual centre gap and menu-bar-height fallback. There is no floating-pill presentation or vertical gap below the menu bar. The centred overlay does not reserve system menu-bar space, so long application menus and crowded status icons remain a possible overlap.

Expanded: a roughly 420pt-wide panel, with a title and collapse control; a subscription section for plan, weekly remaining, weekly reset, credits and available reset credits with their local expiration dates; a separated usage section for Today, MTD and estimated cost; then collection timing and RAM/disk. Use rows and separators, not nested cards. Preserve unusual model-specific quota fields rather than dropping them. Group resets by expiration, distinguish no expiration from unknown dates, and keep long expiration lists scrollable without enlarging the whole panel. Explain incomplete pricing and estimated cost in a quiet footer.

## Interaction

Click compact summary or menu-bar icon to toggle details. Command-Control-U offers a global shortcut; menu actions remain available if registration fails. A second click at either original compact-wing position collapses the expanded island, including the reserved notch/menu-bar band above the detail content. A click on non-interactive detail text or empty space also collapses, as do Escape and the existing collapse button. Child buttons retain their own actions, and scrolling is not a collapse gesture. Refresh uses the existing collector's cache, never forces an extra scan. Open Dashboard opens the existing local dashboard URL. No hover-only actions, auto-expansion, permission prompts or refresh notifications.

## Motion and states

Short ease-out presentation changes, no bounce or refresh animation. Respect Reduce Motion. Cover loading, missing cache, malformed cache, collector failure, stale account quota, incomplete pricing and unavailable resources. Never render absent data as zero. Preserve cached data only with an explicit warning.
