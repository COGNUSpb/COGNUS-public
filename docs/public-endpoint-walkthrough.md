# Public Endpoint Walkthrough

This guide documents the published access path for the current COGNUS baseline and the main screens that can be inspected from the web interface.

## Published Access

- Public endpoint: `http://200.137.197.215:8081`
- Access mode: self-registration is enabled for the `operator` role.
- Primary use in this guide: inspect the currently implemented operator flow and its published screens.

## Scope Of The Published Endpoint

The public endpoint is intended to expose the current baseline already described in the article and in the repository documentation. Through it, the user can:

- register a public `operator` account;
- log in and inspect the overview dashboard;
- navigate the `Provisioning automation and lifecycle` area;
- inspect the guided SSH provisioning cockpit and the organization workspace views;
- inspect runtime-oriented views such as official runtime inspection, logs, and control-plane support screens.

Real SSH-backed provisioning remains possible only when the user supplies a target Linux host together with the corresponding host/IP and access credentials. Those target credentials are external to the public artifact.

## Recommended Walkthrough

### 1. Open The Login Screen

Access the published endpoint in a web browser:

```text
http://200.137.197.215:8081
```

The first screen is the login page of the published dashboard.

![Published login screen](printscreens/1-login.png)

What this screen provides:

- the public entry point of the published dashboard;
- the `Log in` and `Register` tabs in the same access card;
- the authentication fields required for a previously created public operator account;
- the language selector icon in the top-right corner.

What to observe:

- the login page is intentionally minimal and exposes only the public operator access path;
- the same access card is reused for registration, which reduces friction for first-time inspection;
- the published endpoint is suitable both for direct authentication and for first access through self-registration.

### 2. Register A Public Operator Account

Use the registration flow to create a public `operator` account. This is the expected entry point for someone inspecting the published baseline without private bootstrap credentials.

![Public operator registration](printscreens/2-register.png)

What this screen provides:

- a public registration flow limited to the `operator` role;
- explicit notice that this path does not create organizations or administrator accounts;
- the form fields needed for first access:
  `Operator name`, `Email`, `Password`, and password confirmation.

What to observe:

- the registration scope is deliberately constrained to a public operator account;
- this keeps the published endpoint usable without exposing private bootstrap credentials;
- once registration is complete, the same user can authenticate and inspect the implemented baseline from the dashboard.

### 3. Inspect The Overview Dashboard

After authentication, confirm access to the overview dashboard and inspect the current operational summary presented by the system.

![Overview dashboard](printscreens/5-dashboard.png)

What this screen provides:

- a high-level operational summary for the currently visible organizations;
- summary cards for active organizations, live telemetry, local snapshots, freshness, channels and chaincodes, and pending alerts;
- an organization overview area with filters and a card-style summary per organization.

What to observe:

- the dashboard is not only navigational; it is also an auditable summary of the current published baseline;
- the organization card consolidates operational context such as latest snapshot, primary host visibility, peers, orderers, channels, and chaincodes;
- the filters and refresh controls reinforce that this screen is meant for repeated operational inspection rather than only first-time onboarding.

### 4. Open Provisioning And Lifecycle Automation

From the navigation menu, open `Provisioning automation and lifecycle` (pt-BR UI label: `Automação de provisão e lifecycle`) and then the SSH-oriented infrastructure provisioning flow. This is the main entry point for the implemented provisioning baseline described in the article.

![Provisioning and lifecycle entry point](printscreens/3-provisioning-automation-and-lifecycle--infraestruture-provisioning-via-ssh.png)

What this screen provides:

- the dedicated entry point for `Infrastructure Provisioning via SSH`;
- the declared technical scope of the implemented flow;
- the statement of the required operational order for the wizard;
- direct access to the guided cockpit and the provisioning history.

What to observe:

- the screen explicitly states that the mandatory scope is `external-linux` plus Linux VM;
- it also declares the full dependency chain of the flow:
  Infra/VM ready -> Organizations -> Nodes -> Business Groups -> Channels -> Install Chaincodes;
- the `Provisioning history` area makes it clear that the flow is not only interactive, but also traceable across prior runs.

### 5. Inspect The Guided SSH Provisioning Cockpit

The guided cockpit is the central screen for the SSH-backed provisioning journey. It exposes the required order of the workflow, the fields that define the topology, the technical gates that block unsafe progress, and the audit context later correlated by `change_id` and `run_id`.

![Guided SSH provisioning cockpit](printscreens/4.1-guided-SSH-provisioning-cockpit-infraAndPreflight-organizations-nodes-businessgroups-channels-chaincodes.png)

The workflow is intentionally sequential:

1. Infra and preflight
2. Organizations
3. Nodes (peers/orderers)
4. Business Groups
5. Channels
6. Install Chaincodes (`.tar.gz`)

Below is the practical meaning of each stage and the fields expected in the wizard.

#### 5.1 Infra And Preflight

This first stage registers the Linux machines that will be reached through SSH and runs the technical preflight that unlocks the rest of the flow.

Main fields:

- `environment`:
  optional environment profile, typically `dev-external-linux` by default, with the option to switch to `hml` or `prod`.
- `change_id`:
  generated by the wizard and used later to correlate the modeled topology and the published execution context.
- `host_address`:
  the IP address or DNS name of the Linux machine.
- `ssh_user`:
  the Linux user used by SSH.
- `ssh_port`:
  the SSH port, typically `22`.
- `docker_port`:
  optional; defaults to `2376` when used.
- `SSH key per machine (.pem)`:
  local private key uploaded for that machine. Without the `.pem` file, the preflight remains blocked.

What the preflight validates:

- whether the host has enough SSH connection data;
- whether the host is reachable;
- whether the current machine state is safe to continue;
- whether active containers may cause overwrite risk.

The flow only enables `Organizations` when all registered machines are marked as ready and no container overwrite risk remains active.

#### 5.2 Organizations

This stage models the minimum organization topology and the service-to-host mapping. The next stages depend on this information, so this is where most of the topology contract is defined.

Main fields per organization:

- `Organization Name`
- `Domain`
- `Label`
- `Network API Host`
- `Network API Port`
- `CA Mode`:
  `internal` or `external`.
- `CA Name`
- `CA Host`
- `CA Port`
- `CA User`
- `CA Password Ref`
- `CA Host Ref`
- `Peer Host Ref`
- `Peer Port Base`
- `Orderer Host Ref`
- `Orderer Port Base`
- `Couch Host Ref`
- `Couch Port`
- `Couch Database`
- `Couch Admin User`
- `Couch Admin Password Ref`
- `API Gateway Host Ref`
- `API Gateway Port`
- `API Gateway Route Prefix`
- `API Gateway Auth Ref`
- `NetAPI Host Ref`
- `NetAPI Route Prefix`
- `NetAPI Access Ref`

Important behavior:

- empty non-critical fields receive automatic defaults that can be adjusted later;
- host reference fields must point to hosts registered in `Infra and preflight`;
- secrets are expected as secure references such as `vault://`, `secret://`, or `ref://`, not plaintext values.

The flow only enables `Nodes` when the organization topology is complete and the required component-to-host mapping is valid.

#### 5.3 Nodes (Peers And Orderers)

This stage does not remap hosts. Instead, it defines the minimum node volume per organization using the host mapping already declared in `Organizations`.

Main fields per organization:

- `Peers`
- `Orderers`

Gate rule:

- each organization must have at least one node, either a peer or an orderer, before the flow enables `Business Groups`.

#### 5.4 Business Groups

This stage defines the operational grouping used by the next channel and install stages.

Main fields:

- `Name`
- `Network ID`
- `Description`

Important behavior:

- if `Network ID` is left empty, the wizard proposes one automatically from the business group name.

The flow only enables `Channels` after a valid business group is defined.

#### 5.5 Channels

This stage creates the logical channels that will later receive chaincode installation.

Main fields per channel:

- `Channel name`
- `Organizations (csv, optional)`

Important behavior:

- if the organization list is left empty, the wizard uses the currently modeled organizations by default;
- the association can be refined later.

The flow only enables `Install Chaincodes` after at least one valid channel exists.

#### 5.6 Install Chaincodes

This stage registers the package artifact that the orchestrator will later reuse during the runbook execution.

Main fields per install:

- `Chaincode package (.tar.gz)`
- `Chaincode name`
- `Target channel`

Important behavior:

- each install requires one uploaded `.tar.gz` package;
- the chaincode name is the logical identifier used by the modeled install;
- the selected channel binds the package to the target channel context.

The same screen also exposes complementary operational panels for API registration and post-creation incremental expansion, but the minimum guided path required by the wizard is the six-stage sequence listed above.

### 6. Inspect The Organization Workspace

The organization workspace exposes views related to infrastructure access and runtime status. The published baseline includes screens for VM access gating and activation state.

Blocked or pending VM access state:

![Organization workspace with blocked VMs](printscreens/6-organization-workspace-blockedVMs.png)

Enabled VM access state:

![Organization workspace with enabled VMs](printscreens/7-organization-workspace-enableVMs.png)

What this workspace provides:

- an organization-scoped operational cockpit;
- buttons for governed expansion actions such as `Add peer`, `Add orderer`, `Add channel`, and `Add chaincode`;
- a VM access area that controls whether protected host data becomes visible in the organization workspace;
- the main operational blocks for peers, orderers, channels, chaincodes, evidence, and control-plane/support services.

What to observe in the blocked view:

- VM access is still protected, so the primary host remains hidden;
- runtime navigation remains partially visible for audit and context, but sensitive host information is still withheld;
- this view demonstrates the controlled exposure model of the workspace.

What to observe in the enabled view:

- once VM access is released, the validated VM count changes and the primary host becomes visible;
- the same workspace structure is preserved, but with broader runtime visibility;
- this distinction is useful to demonstrate that the workspace is not a flat dashboard: it reacts to access state and operational trust gates.

### 7. Inspect Runtime And Evidence-Oriented Views

The current baseline also exposes runtime-oriented inspection and operational evidence views, including official runtime inspection and logs.

Official runtime inspection:

![Official runtime inspection](printscreens/8-organization-workspace-official-runtime-inspectionv1.png)

Logs view:

![Organization workspace logs](printscreens/9-organization-workspace-logs.png)

What the official runtime inspection provides:

- a component-scoped technical inspection view, in the screenshot centered on `peer0-inf-ufg`;
- correlation metadata such as `run_id`, `change_id`, organization, host, component type, container name, image, and inspection source;
- runtime health and freshness fields such as health status, running state, restart count, and update time;
- a technical summary generated from the current runtime inspection result.

What to observe in the official inspection:

- this screen makes the audit context explicit by correlating the inspected component with `run_id` and `change_id`;
- it also distinguishes the inspected host, component type, and image used in the published runtime;
- the presence of cache/freshness information indicates that this is an operational inspection view, not only a static screenshot of container metadata.

What the logs view provides:

- a sanitized runtime log stream associated with the inspected component;
- a latest-log following mode;
- label metadata published in the runtime below the log panel.

What to observe in the logs view:

- the screen connects observable runtime behavior with the same inspected component shown in the technical inspection;
- the labels section helps relate runtime evidence to the orchestrated context;
- this supports the claim that the tool records and exposes operational evidence rather than only topology modeling.

### 8. Inspect Control Plane And Support Views

The organization workspace also includes control-plane and support-oriented views that complement the published operational baseline.

![Organization workspace control plane and support](printscreens/10-organization-workspace-controle-plane-and-support.png)

What this screen provides:

- a grouped view of support services associated with the organization;
- a control-plane section for identity, gateway, and API-related services;
- a support-services section for persistence and runtime components.

What to observe:

- the screen separates control-plane elements from support/runtime elements, which helps explain the layered structure of the tool;
- the listed items include API gateway, CA, NetAPI, chaincode runtime, and CouchDB;
- each item is shown together with host association and operational tags such as `running`, `supporting`, `critical`, or `degraded`, which makes the screen useful both for topology understanding and for operational triage.

## Optional Real Provisioning

If the user intends to go beyond screen inspection and exercise SSH-backed provisioning, the required inputs are external to the public artifact:

- target host/IP;
- SSH user;
- password or private key;
- Linux host with Docker support or permission for Docker installation.

This keeps the published repository free of private credentials while still exposing the implemented product flow and the public access path required for assessment.

## Related Material

- [README.md](../README.md)
- [ARTIFACT.md](../ARTIFACT.md)
- [PREREQUISITES.md](../PREREQUISITES.md)
- [auto-provisioning.md](auto-provisioning.md)
