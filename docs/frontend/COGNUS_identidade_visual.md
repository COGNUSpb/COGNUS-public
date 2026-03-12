# COGNUS: Visual Identity And Design System (v2.0)

Official visual identity document for the COGNUS frontend, aligned with the operational use of the orchestrator across control plane, console, and observability layers.

## 1) Visual Direction

The frontend should communicate three attributes:

1. **Operational reliability** with explicit state and low ambiguity.
2. **Auditable governance** with visible evidence and traceable history.
3. **Network coordination** with organizations, channels, and components presented as a connected system.

Application references:
- `docs/visual-identity.md`
- `docs/primary-work-article/low-level-architecture-v2-no-notes.puml`

## 2) Color Palette

### 2.1 Brand
| Token | HEX | Usage |
|---|---:|---|
| `brand.night` | `#1B2559` | institutional backgrounds and navigation |
| `brand.link` | `#2F6BFF` | primary actions, links, and focus |
| `brand.network` | `#2CB6A6` | connectivity, topology, and network health |
| `brand.star` | `#7C3AED` | governance and evidence highlights |

### 2.2 Neutrals
| Token | HEX |
|---|---:|
| `neutral.950` | `#0B1022` |
| `neutral.900` | `#111827` |
| `neutral.700` | `#374151` |
| `neutral.500` | `#6B7280` |
| `neutral.200` | `#E5E7EB` |
| `neutral.100` | `#F3F4F6` |
| `neutral.0` | `#FFFFFF` |

### 2.3 Semantic status
| Token | HEX | Semantics |
|---|---:|---|
| `status.success` | `#16A34A` | completed or approved |
| `status.info` | `#0284C7` | running or informational |
| `status.warning` | `#D97706` | risk or attention required |
| `status.danger` | `#DC2626` | failure or blocked state |
| `status.muted` | `#9CA3AF` | inactive or archived |

## 3) Typography

- UI and content: `Inter`
- IDs, hashes, endpoints, and logs: `JetBrains Mono`
- Titles: weight 600
- Operational body text: 14-16px with line-height 1.45-1.55

## 4) Layout And Density

- 12-column grid for desktop.
- Spacing scale in multiples of 4px.
- Default density: **compact** for operational screens.
- Main layout elements:
  - sidebar and contextual header with organization, channel, environment, and user;
  - work area with cards and tables;
  - side panel for evidence and diagnostics.

## 5) Required Product Components

1. **`Change Request` stepper**
2. **Evidence timeline**
3. **Operational table with organization and channel filters**
4. **Guardrail and SLO panel**
5. **Network topology with drill-down**
6. **Incident and alert panel**

## 6) Visual Mapping Of Critical States

- `CR Draft` -> `status.muted`
- `CR Pending Approval` -> `status.info`
- `CR Running` -> `status.info`
- `CR Blocked` -> `status.warning` or `status.danger`
- `CR Completed` -> `status.success`
- `CR Failed/Rolled Back` -> `status.danger`

## 7) Data And Visualization Guidelines

- Every operational view must expose `change-id`.
- Observability charts must accept organization and channel filters.
- Alerts must show severity, impact, and suggested action.
- Evidence inspection must always remain reachable from the current screen.

## 8) Accessibility And Visual Safety

- Minimum 4.5:1 contrast for normal text.
- Visible focus on interactive elements.
- Status must never be encoded by color alone; include text or icon.
- Avoid very small text in dense panels, especially below 12px.

## 9) Design Tokens (CSS Base)

```css
:root {
  --brand-night: #1B2559;
  --brand-link: #2F6BFF;
  --brand-network: #2CB6A6;
  --brand-star: #7C3AED;

  --neutral-950: #0B1022;
  --neutral-900: #111827;
  --neutral-700: #374151;
  --neutral-500: #6B7280;
  --neutral-200: #E5E7EB;
  --neutral-100: #F3F4F6;
  --neutral-0: #FFFFFF;

  --status-success: #16A34A;
  --status-info: #0284C7;
  --status-warning: #D97706;
  --status-danger: #DC2626;
  --status-muted: #9CA3AF;

  --radius-sm: 8px;
  --radius-md: 12px;
  --radius-lg: 16px;

  --s-1: 4px;
  --s-2: 8px;
  --s-3: 12px;
  --s-4: 16px;
  --s-5: 20px;
  --s-6: 24px;
  --s-8: 32px;
}
```

## 10) Frontend PR Checklist

- [ ] uses tokens instead of ad hoc colors;
- [ ] exposes operational context (organization, channel, environment);
- [ ] applies correct status semantics;
- [ ] shows `change-id` and evidence access where applicable;
- [ ] meets accessibility contrast and focus requirements.

---

**Version:** 2.0  
**Scope:** visual identity and design rules for the operational COGNUS frontend.
