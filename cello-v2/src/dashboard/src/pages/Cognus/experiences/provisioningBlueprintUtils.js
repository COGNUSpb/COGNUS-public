import yaml from 'js-yaml';

export const BLUEPRINT_RUNTIME_SCHEMA_VERSION = '1.0.0';

export const BLUEPRINT_SCOPE_CONSTRAINTS = Object.freeze({
  provider: 'external',
  computeTarget: 'vm_linux',
  osFamily: 'linux',
});

const BLUEPRINT_REQUIRED_BLOCKS = [
  { key: 'network', expectedType: 'object' },
  { key: 'orgs', expectedType: 'array' },
  { key: 'channels', expectedType: 'array' },
  { key: 'nodes', expectedType: 'array' },
  { key: 'policies', expectedType: 'array' },
  { key: 'environment_profile', expectedType: 'object' },
];

const SEMVER_REGEX = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:[-+][0-9A-Za-z.-]+)?$/;
const ISO_UTC_REGEX = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3,6})?Z$/;

const ISSUE_LEVEL_ORDER = {
  error: 0,
  warning: 1,
  hint: 2,
};

const isPlainObject = value => Boolean(value) && typeof value === 'object' && !Array.isArray(value);

const normalizeTextValue = value => {
  if (typeof value !== 'string') {
    return '';
  }
  return value.trim();
};

const toIsoUtc = (date = new Date()) => date.toISOString().replace(/\.\d{3}Z$/, 'Z');

const parseSemver = version => {
  if (!SEMVER_REGEX.test(version || '')) {
    return null;
  }

  const [major, minor, patch] = version
    .split('-', 1)[0]
    .split('+', 1)[0]
    .split('.')
    .map(part => Number(part));

  return {
    major,
    minor,
    patch,
  };
};

const compareSemver = (leftVersion, rightVersion) => {
  const left = parseSemver(leftVersion);
  const right = parseSemver(rightVersion);

  if (!left || !right) {
    return 0;
  }

  if (left.major !== right.major) {
    return left.major > right.major ? 1 : -1;
  }
  if (left.minor !== right.minor) {
    return left.minor > right.minor ? 1 : -1;
  }
  if (left.patch !== right.patch) {
    return left.patch > right.patch ? 1 : -1;
  }
  return 0;
};

const sortIssues = issues =>
  [...issues].sort((left, right) => {
    const leftOrder = ISSUE_LEVEL_ORDER[left.level] || 99;
    const rightOrder = ISSUE_LEVEL_ORDER[right.level] || 99;

    if (leftOrder !== rightOrder) {
      return leftOrder - rightOrder;
    }

    if (left.path !== right.path) {
      return left.path > right.path ? 1 : -1;
    }

    if (left.code !== right.code) {
      return left.code > right.code ? 1 : -1;
    }

    if (left.message !== right.message) {
      return left.message > right.message ? 1 : -1;
    }

    return 0;
  });

const createIssue = (level, code, path, message) => ({
  level,
  code,
  path,
  message,
});

const normalizeObjectDeep = value => {
  if (Array.isArray(value)) {
    return value.map(item => normalizeObjectDeep(item));
  }

  if (!isPlainObject(value)) {
    return value;
  }

  return Object.keys(value)
    .sort()
    .reduce((accumulator, key) => {
      accumulator[key] = normalizeObjectDeep(value[key]);
      return accumulator;
    }, {});
};

const canonicalizeValue = value =>
  JSON.stringify(normalizeObjectDeep(value === undefined ? null : value));

export const canonicalizeBlueprint = blueprint => JSON.stringify(normalizeObjectDeep(blueprint));

export const inferBlueprintDocumentFormat = fileName => {
  const normalizedName = String(fileName || '').toLowerCase();

  if (normalizedName.endsWith('.json')) {
    return 'json';
  }

  if (normalizedName.endsWith('.yaml') || normalizedName.endsWith('.yml')) {
    return 'yaml';
  }

  return 'auto';
};

export const createBlueprintDraftTemplate = (overrides = {}) => {
  const now = toIsoUtc();

  const template = {
    schema_name: 'cognus-blueprint',
    schema_version: BLUEPRINT_RUNTIME_SCHEMA_VERSION,
    blueprint_version: '1.0.0',
    created_at: now,
    updated_at: now,
    network: {
      network_id: 'consortium-dev',
      display_name: 'Consortium DEV',
      domain: 'cognus.dev.local',
    },
    orgs: [
      {
        org_id: 'infufg',
        display_name: 'INF UFG',
        msp_id: 'InfufgMSP',
        domain: 'infufg.cognus.local',
        roles: ['peer', 'ca'],
      },
      {
        org_id: 'anatel',
        display_name: 'ANATEL',
        msp_id: 'AnatelMSP',
        domain: 'anatel.cognus.local',
        roles: ['peer', 'ca'],
      },
    ],
    channels: [
      {
        channel_id: 'ops-channel',
        type: 'ops',
        members: ['infufg', 'anatel'],
      },
    ],
    nodes: [
      {
        node_id: 'peer0-infufg',
        org_id: 'infufg',
        node_type: 'peer',
        host_ref: 'vm-dev-01',
        provider: 'external',
        os_family: 'linux',
        ports: [7051, 7052],
      },
      {
        node_id: 'peer0-anatel',
        org_id: 'anatel',
        node_type: 'peer',
        host_ref: 'vm-dev-02',
        provider: 'external',
        os_family: 'linux',
        ports: [8051, 8052],
      },
    ],
    policies: [
      {
        policy_id: 'policy-channel-ops',
        scope: 'channel',
        target: {
          channel_id: 'ops-channel',
        },
        rules: {
          allow: ['lifecycle.approve', 'lifecycle.commit'],
          deny: ['lifecycle.force-commit'],
        },
      },
    ],
    environment_profile: {
      profile_id: 'dev-external-linux',
      stage: 'dev',
      provider: 'external',
      compute_target: 'vm_linux',
      os_family: 'linux',
      deployment_stack: 'docker',
      stack_compatibility: ['docker', 'hybrid'],
    },
  };

  return {
    ...template,
    ...overrides,
  };
};

const assertBlueprintObject = parsedDocument => {
  if (!isPlainObject(parsedDocument)) {
    throw new Error('O documento deve representar um objeto de blueprint no nível raiz.');
  }
  return parsedDocument;
};

export const parseBlueprintDocument = (content, formatHint = 'auto') => {
  const rawContent = String(content || '');
  const normalizedContent = rawContent.trim();

  if (!normalizedContent) {
    throw new Error('O arquivo está vazio.');
  }

  const parseErrors = [];
  const shouldTryJsonFirst =
    formatHint === 'json' ||
    (formatHint === 'auto' &&
      (normalizedContent.startsWith('{') || normalizedContent.startsWith('[')));

  if (shouldTryJsonFirst) {
    try {
      return assertBlueprintObject(JSON.parse(normalizedContent));
    } catch (error) {
      parseErrors.push(`JSON: ${error.message}`);
      if (formatHint === 'json') {
        throw new Error(`Falha ao interpretar JSON. ${parseErrors.join(' | ')}`);
      }
    }
  }

  try {
    return assertBlueprintObject(yaml.safeLoad(normalizedContent));
  } catch (error) {
    parseErrors.push(`YAML: ${error.message}`);
  }

  if (!shouldTryJsonFirst) {
    try {
      return assertBlueprintObject(JSON.parse(normalizedContent));
    } catch (error) {
      parseErrors.push(`JSON: ${error.message}`);
    }
  }

  throw new Error(
    `Não foi possível interpretar o blueprint como JSON/YAML válido. ${parseErrors.join(' | ')}`
  );
};

export const serializeBlueprintDocument = (blueprint, format = 'json') => {
  if (format === 'yaml') {
    return yaml.safeDump(blueprint, {
      noRefs: true,
      lineWidth: 120,
      sortKeys: true,
    });
  }

  return JSON.stringify(normalizeObjectDeep(blueprint), null, 2);
};

export const lintBlueprintDraft = (
  blueprint,
  {
    runtimeSchemaVersion = BLUEPRINT_RUNTIME_SCHEMA_VERSION,
    scopeConstraints = BLUEPRINT_SCOPE_CONSTRAINTS,
  } = {}
) => {
  const issues = [];

  if (!isPlainObject(blueprint)) {
    const errorIssue = createIssue(
      'error',
      'invalid_blueprint_root',
      '$',
      'Blueprint deve ser um objeto com metadados e blocos de topologia.'
    );

    return {
      valid: false,
      schemaVersion: '',
      resolvedSchemaVersion: runtimeSchemaVersion,
      blueprintVersion: '',
      createdAt: '',
      updatedAt: '',
      summary: {
        orgs: 0,
        channels: 0,
        nodes: 0,
        policies: 0,
      },
      errors: [errorIssue],
      warnings: [],
      hints: [],
      issues: [errorIssue],
    };
  }

  const metadata = {
    schemaName: normalizeTextValue(blueprint.schema_name),
    schemaVersion: normalizeTextValue(blueprint.schema_version),
    blueprintVersion: normalizeTextValue(blueprint.blueprint_version),
    createdAt: normalizeTextValue(blueprint.created_at),
    updatedAt: normalizeTextValue(blueprint.updated_at),
  };

  if (!metadata.schemaName) {
    issues.push(
      createIssue('error', 'required_schema_name', 'schema_name', 'schema_name é obrigatório.')
    );
  }

  if (!metadata.schemaVersion) {
    issues.push(
      createIssue(
        'error',
        'required_schema_version',
        'schema_version',
        'schema_version é obrigatório em SemVer.'
      )
    );
  } else if (!SEMVER_REGEX.test(metadata.schemaVersion)) {
    issues.push(
      createIssue(
        'error',
        'invalid_schema_version',
        'schema_version',
        'schema_version deve seguir MAJOR.MINOR.PATCH.'
      )
    );
  }

  if (!metadata.blueprintVersion) {
    issues.push(
      createIssue(
        'error',
        'required_blueprint_version',
        'blueprint_version',
        'blueprint_version é obrigatório em SemVer.'
      )
    );
  } else if (!SEMVER_REGEX.test(metadata.blueprintVersion)) {
    issues.push(
      createIssue(
        'error',
        'invalid_blueprint_version',
        'blueprint_version',
        'blueprint_version deve seguir MAJOR.MINOR.PATCH.'
      )
    );
  }

  if (!metadata.createdAt || !ISO_UTC_REGEX.test(metadata.createdAt)) {
    issues.push(
      createIssue(
        'error',
        'invalid_created_at',
        'created_at',
        'created_at deve ser timestamp UTC ISO-8601.'
      )
    );
  }

  if (!metadata.updatedAt || !ISO_UTC_REGEX.test(metadata.updatedAt)) {
    issues.push(
      createIssue(
        'error',
        'invalid_updated_at',
        'updated_at',
        'updated_at deve ser timestamp UTC ISO-8601.'
      )
    );
  }

  let resolvedSchemaVersion = runtimeSchemaVersion;
  if (metadata.schemaVersion && SEMVER_REGEX.test(metadata.schemaVersion)) {
    const comparisonWithRuntime = compareSemver(metadata.schemaVersion, runtimeSchemaVersion);

    if (comparisonWithRuntime > 0) {
      issues.push(
        createIssue(
          'error',
          'schema_version_newer_than_runtime',
          'schema_version',
          `schema_version ${metadata.schemaVersion} excede o runtime ${runtimeSchemaVersion}.`
        )
      );
    } else if (comparisonWithRuntime < 0) {
      issues.push(
        createIssue(
          'hint',
          'schema_version_older_compatible',
          'schema_version',
          `schema_version ${metadata.schemaVersion} é compatível e será resolvida para ${runtimeSchemaVersion}.`
        )
      );
    }

    const schemaMajor = parseSemver(metadata.schemaVersion);
    const runtimeMajor = parseSemver(runtimeSchemaVersion);
    if (schemaMajor && runtimeMajor && schemaMajor.major > runtimeMajor.major) {
      issues.push(
        createIssue(
          'error',
          'schema_version_major_ahead',
          'schema_version',
          `Major ${schemaMajor.major} não suportado pelo runtime ${runtimeSchemaVersion}.`
        )
      );
    }

    resolvedSchemaVersion =
      comparisonWithRuntime > 0 ? metadata.schemaVersion : runtimeSchemaVersion;
  }

  BLUEPRINT_REQUIRED_BLOCKS.forEach(block => {
    const value = blueprint[block.key];
    const expectedType = block.expectedType;

    if (value === undefined || value === null) {
      issues.push(
        createIssue(
          'error',
          'required_block_missing',
          block.key,
          `Bloco obrigatório '${block.key}' não informado.`
        )
      );
      return;
    }

    if (expectedType === 'array' && !Array.isArray(value)) {
      issues.push(
        createIssue(
          'error',
          'required_block_invalid_type',
          block.key,
          `Bloco '${block.key}' deve ser uma lista.`
        )
      );
      return;
    }

    if (expectedType === 'object' && !isPlainObject(value)) {
      issues.push(
        createIssue(
          'error',
          'required_block_invalid_type',
          block.key,
          `Bloco '${block.key}' deve ser um objeto.`
        )
      );
      return;
    }

    if (expectedType === 'array' && value.length === 0) {
      issues.push(
        createIssue(
          'error',
          'required_block_empty',
          block.key,
          `Bloco '${block.key}' não pode ser vazio.`
        )
      );
    }
  });

  const orgs = Array.isArray(blueprint.orgs) ? blueprint.orgs : [];
  const channels = Array.isArray(blueprint.channels) ? blueprint.channels : [];
  const nodes = Array.isArray(blueprint.nodes) ? blueprint.nodes : [];
  const policies = Array.isArray(blueprint.policies) ? blueprint.policies : [];
  const environmentProfile = isPlainObject(blueprint.environment_profile)
    ? blueprint.environment_profile
    : null;

  const orgIds = new Set();
  const peerNodesByOrg = {};

  orgs.forEach((org, index) => {
    const orgPath = `orgs[${index}]`;

    if (!isPlainObject(org)) {
      issues.push(
        createIssue('error', 'invalid_org_object', orgPath, 'Cada item de orgs deve ser um objeto.')
      );
      return;
    }

    const orgId = normalizeTextValue(org.org_id);
    if (!orgId) {
      issues.push(
        createIssue('error', 'required_org_id', `${orgPath}.org_id`, 'org_id é obrigatório.')
      );
    } else if (orgIds.has(orgId)) {
      issues.push(
        createIssue(
          'error',
          'duplicate_org_id',
          `${orgPath}.org_id`,
          `org_id '${orgId}' está duplicado.`
        )
      );
    } else {
      orgIds.add(orgId);
    }

    const roles = Array.isArray(org.roles) ? org.roles : [];
    if (roles.length === 0) {
      issues.push(
        createIssue(
          'warning',
          'org_roles_missing',
          `${orgPath}.roles`,
          'roles deveria declarar ao menos peer, orderer ou ca.'
        )
      );
    }
  });

  nodes.forEach((node, index) => {
    const nodePath = `nodes[${index}]`;

    if (!isPlainObject(node)) {
      issues.push(
        createIssue(
          'error',
          'invalid_node_object',
          nodePath,
          'Cada item de nodes deve ser um objeto.'
        )
      );
      return;
    }

    const nodeId = normalizeTextValue(node.node_id);
    if (!nodeId) {
      issues.push(
        createIssue('error', 'required_node_id', `${nodePath}.node_id`, 'node_id é obrigatório.')
      );
    }

    const orgId = normalizeTextValue(node.org_id);
    if (!orgId) {
      issues.push(
        createIssue(
          'error',
          'required_node_org_id',
          `${nodePath}.org_id`,
          'org_id é obrigatório em nodes.'
        )
      );
    } else if (orgIds.size > 0 && !orgIds.has(orgId)) {
      issues.push(
        createIssue(
          'error',
          'node_org_not_found',
          `${nodePath}.org_id`,
          `org_id '${orgId}' não existe em orgs.`
        )
      );
    }

    const nodeType = normalizeTextValue(node.node_type).toLowerCase();
    if (nodeType === 'peer' && orgId) {
      peerNodesByOrg[orgId] = (peerNodesByOrg[orgId] || 0) + 1;
    }

    if (!normalizeTextValue(node.host_ref)) {
      issues.push(
        createIssue(
          'error',
          'required_host_ref',
          `${nodePath}.host_ref`,
          'host_ref é obrigatório para execução em VM Linux.'
        )
      );
    }

    const nodeProvider = normalizeTextValue(
      node.provider || (isPlainObject(node.runtime) ? node.runtime.provider : '')
    ).toLowerCase();

    if (nodeProvider && nodeProvider !== scopeConstraints.provider) {
      issues.push(
        createIssue(
          'error',
          'node_provider_not_supported',
          `${nodePath}.provider`,
          `Apenas provider '${scopeConstraints.provider}' é suportado neste ciclo.`
        )
      );
    }

    const nodeOs = normalizeTextValue(
      node.os_family ||
        node.os ||
        node.platform ||
        (isPlainObject(node.runtime) ? node.runtime.os_family || node.runtime.os : '')
    ).toLowerCase();

    if (nodeOs && !nodeOs.includes(scopeConstraints.osFamily)) {
      issues.push(
        createIssue(
          'error',
          'node_os_not_supported',
          `${nodePath}.os_family`,
          `Apenas hosts ${scopeConstraints.osFamily} são aceitos neste ciclo.`
        )
      );
    }
  });

  channels.forEach((channel, index) => {
    const channelPath = `channels[${index}]`;

    if (!isPlainObject(channel)) {
      issues.push(
        createIssue(
          'error',
          'invalid_channel_object',
          channelPath,
          'Cada item de channels deve ser um objeto.'
        )
      );
      return;
    }

    const channelId = normalizeTextValue(channel.channel_id);
    if (!channelId) {
      issues.push(
        createIssue(
          'error',
          'required_channel_id',
          `${channelPath}.channel_id`,
          'channel_id é obrigatório.'
        )
      );
    }

    const members = Array.isArray(channel.members) ? channel.members : [];
    if (members.length === 0) {
      issues.push(
        createIssue(
          'error',
          'channel_members_missing',
          `${channelPath}.members`,
          'members é obrigatório em channels.'
        )
      );
      return;
    }

    members.forEach((member, memberIndex) => {
      const memberId = normalizeTextValue(member);
      const memberPath = `${channelPath}.members[${memberIndex}]`;

      if (!memberId) {
        issues.push(
          createIssue(
            'error',
            'channel_member_empty',
            memberPath,
            'Cada member deve conter org_id válido.'
          )
        );
        return;
      }

      if (orgIds.size > 0 && !orgIds.has(memberId)) {
        issues.push(
          createIssue(
            'error',
            'channel_member_org_not_found',
            memberPath,
            `Membro '${memberId}' não existe em orgs.`
          )
        );
      }

      if (!peerNodesByOrg[memberId]) {
        issues.push(
          createIssue(
            'error',
            'channel_member_without_peer_node',
            memberPath,
            `Membro '${memberId}' não possui node peer elegível para participação.`
          )
        );
      }
    });
  });

  policies.forEach((policy, index) => {
    const policyPath = `policies[${index}]`;

    if (!isPlainObject(policy)) {
      issues.push(
        createIssue(
          'error',
          'invalid_policy_object',
          policyPath,
          'Cada item de policies deve ser um objeto.'
        )
      );
      return;
    }

    if (!normalizeTextValue(policy.policy_id)) {
      issues.push(
        createIssue(
          'error',
          'required_policy_id',
          `${policyPath}.policy_id`,
          'policy_id é obrigatório.'
        )
      );
    }

    if (!normalizeTextValue(policy.scope)) {
      issues.push(
        createIssue(
          'warning',
          'policy_scope_missing',
          `${policyPath}.scope`,
          'scope deveria indicar network, org, channel ou operation.'
        )
      );
    }
  });

  if (environmentProfile) {
    const profilePath = 'environment_profile';
    const provider = normalizeTextValue(environmentProfile.provider).toLowerCase();
    const computeTarget = normalizeTextValue(environmentProfile.compute_target).toLowerCase();
    const osFamily = normalizeTextValue(environmentProfile.os_family).toLowerCase();

    if (!provider) {
      issues.push(
        createIssue(
          'error',
          'scope_provider_required',
          `${profilePath}.provider`,
          `provider obrigatório e restrito a '${scopeConstraints.provider}'.`
        )
      );
    } else if (provider !== scopeConstraints.provider) {
      issues.push(
        createIssue(
          'error',
          'scope_provider_not_supported',
          `${profilePath}.provider`,
          `Apenas provider '${scopeConstraints.provider}' é permitido neste ciclo.`
        )
      );
    }

    if (!computeTarget) {
      issues.push(
        createIssue(
          'error',
          'scope_compute_target_required',
          `${profilePath}.compute_target`,
          `compute_target obrigatório e restrito a '${scopeConstraints.computeTarget}'.`
        )
      );
    } else if (computeTarget !== scopeConstraints.computeTarget) {
      issues.push(
        createIssue(
          'error',
          'scope_compute_target_not_supported',
          `${profilePath}.compute_target`,
          `Apenas compute_target '${scopeConstraints.computeTarget}' é permitido.`
        )
      );
    }

    if (!osFamily) {
      issues.push(
        createIssue(
          'warning',
          'scope_os_family_missing',
          `${profilePath}.os_family`,
          `os_family deveria explicitar '${scopeConstraints.osFamily}'.`
        )
      );
    } else if (!osFamily.includes(scopeConstraints.osFamily)) {
      issues.push(
        createIssue(
          'error',
          'scope_os_family_not_supported',
          `${profilePath}.os_family`,
          `Apenas '${scopeConstraints.osFamily}' é suportado neste ciclo.`
        )
      );
    }
  }

  if (!issues.some(issue => issue.level === 'error')) {
    issues.push(
      createIssue(
        'hint',
        'lint_ready_for_publish',
        '$',
        'Lint sem erros bloqueantes. Blueprint apto para publicação.'
      )
    );
  }

  const orderedIssues = sortIssues(issues);

  return {
    valid: !orderedIssues.some(issue => issue.level === 'error'),
    schemaVersion: metadata.schemaVersion,
    resolvedSchemaVersion,
    blueprintVersion: metadata.blueprintVersion,
    createdAt: metadata.createdAt,
    updatedAt: metadata.updatedAt,
    summary: {
      orgs: orgs.length,
      channels: channels.length,
      nodes: nodes.length,
      policies: policies.length,
    },
    errors: orderedIssues.filter(issue => issue.level === 'error'),
    warnings: orderedIssues.filter(issue => issue.level === 'warning'),
    hints: orderedIssues.filter(issue => issue.level === 'hint'),
    issues: orderedIssues,
  };
};

const BUFFER_HEX_PADDING = '00000000';
const HASH_MODULUS = 4294967291;

const seededHash = (input, seed) => {
  let hash = Number(seed) % HASH_MODULUS;

  for (let index = 0; index < input.length; index += 1) {
    const charCode = input.charCodeAt(index);
    hash = (hash * 1664525 + charCode + 1013904223) % HASH_MODULUS;
  }

  return Math.floor(hash)
    .toString(16)
    .slice(-8)
    .padStart(8, '0');
};

const fallbackFingerprint = input => {
  const hashParts = [
    seededHash(input, 0x811c9dc5),
    seededHash(input, 0x27d4eb2f),
    seededHash(input, 0x9e3779b1),
    seededHash(input, 0x85ebca6b),
    seededHash(input, 0xc2b2ae35),
    seededHash(input, 0x165667b1),
    seededHash(input, 0xd3a2646c),
    seededHash(input, 0xfd7046c5),
  ];

  return hashParts.join('');
};

const bufferToHex = digest => {
  const digestBytes = new Uint8Array(digest);

  return Array.from(digestBytes)
    .map(byteValue => (BUFFER_HEX_PADDING + byteValue.toString(16)).slice(-2))
    .join('');
};

const resolveCryptoRuntime = () => {
  if (typeof window !== 'undefined' && window.crypto && window.crypto.subtle) {
    return window.crypto;
  }

  if (typeof global !== 'undefined' && global.crypto && global.crypto.subtle) {
    return global.crypto;
  }

  return null;
};

export const calculateCanonicalFingerprint = async blueprint => {
  const canonicalBlueprint = canonicalizeBlueprint(blueprint);
  const cryptoRuntime = resolveCryptoRuntime();

  if (cryptoRuntime && typeof TextEncoder !== 'undefined') {
    try {
      const encodedPayload = new TextEncoder().encode(canonicalBlueprint);
      const digest = await cryptoRuntime.subtle.digest('SHA-256', encodedPayload);
      return bufferToHex(digest);
    } catch (error) {
      // Fallback determinístico para ambientes sem WebCrypto operacional.
    }
  }

  return fallbackFingerprint(canonicalBlueprint);
};

const DIFF_BLOCKS = [
  { key: 'schema_version', label: 'Schema do blueprint' },
  { key: 'blueprint_version', label: 'Versão do blueprint' },
  { key: 'network', label: 'network' },
  { key: 'orgs', label: 'orgs' },
  { key: 'channels', label: 'channels' },
  { key: 'nodes', label: 'nodes' },
  { key: 'policies', label: 'policies' },
  { key: 'environment_profile', label: 'environment_profile' },
];

const summarizeListValue = value => {
  if (!Array.isArray(value)) {
    return '0 item(ns)';
  }

  const identifiers = value
    .map(item => {
      if (!isPlainObject(item)) {
        return '';
      }

      return item.org_id || item.channel_id || item.node_id || item.policy_id || item.id || '';
    })
    .filter(Boolean)
    .slice(0, 3);

  if (identifiers.length === 0) {
    return `${value.length} item(ns)`;
  }

  return `${value.length} item(ns) · ${identifiers.join(', ')}`;
};

const summarizeObjectValue = value => {
  if (!isPlainObject(value)) {
    return 'não definido';
  }

  const keys = Object.keys(value);

  if (value.profile_id || value.stage) {
    return `${value.profile_id || 'sem profile_id'} · ${value.stage || 'sem stage'}`;
  }

  if (value.network_id || value.display_name) {
    return `${value.network_id || value.display_name} · ${keys.length} campo(s)`;
  }

  return `${keys.length} campo(s)`;
};

const summarizeDiffValue = value => {
  if (Array.isArray(value)) {
    return summarizeListValue(value);
  }

  if (isPlainObject(value)) {
    return summarizeObjectValue(value);
  }

  if (value === undefined || value === null || value === '') {
    return 'não definido';
  }

  return String(value);
};

export const summarizeBlueprintVersionDiff = (baseBlueprint, targetBlueprint) =>
  DIFF_BLOCKS.map(block => {
    const baseValue = baseBlueprint ? baseBlueprint[block.key] : undefined;
    const targetValue = targetBlueprint ? targetBlueprint[block.key] : undefined;

    return {
      key: block.key,
      label: block.label,
      changed: canonicalizeValue(baseValue) !== canonicalizeValue(targetValue),
      baseSummary: summarizeDiffValue(baseValue),
      targetSummary: summarizeDiffValue(targetValue),
    };
  });

export const truncateFingerprint = (fingerprint, previewSize = 12) => {
  const normalizedFingerprint = normalizeTextValue(fingerprint);

  if (!normalizedFingerprint) {
    return '-';
  }

  if (normalizedFingerprint.length <= previewSize * 2) {
    return normalizedFingerprint;
  }

  return `${normalizedFingerprint.slice(0, previewSize)}...${normalizedFingerprint.slice(
    -previewSize
  )}`;
};

const BACKEND_LINT_REQUIRED_FIELDS = [
  'valid',
  'errors',
  'warnings',
  'hints',
  'schema_runtime',
  'resolved_schema_version',
  'fingerprint_sha256',
  'normalized_orgs',
  'normalized_channels',
  'normalized_nodes',
  'normalized_policies',
  'normalized_environment_profile',
  'normalized_identity_baseline',
];

const ensureIssueList = (rawIssues, level, bucketLabel) => {
  if (!Array.isArray(rawIssues)) {
    return [
      createIssue(
        'error',
        'invalid_lint_contract_issue_bucket',
        `$contract.${bucketLabel}`,
        `Bucket '${bucketLabel}' inválido no contrato de lint (${level}).`
      ),
    ];
  }

  return rawIssues.map((rawIssue, index) => {
    if (!isPlainObject(rawIssue)) {
      return createIssue(
        'error',
        'invalid_lint_contract_issue_item',
        `$contract.${bucketLabel}[${index}]`,
        `Item de issue inválido em '${bucketLabel}'.`
      );
    }

    const issueCode =
      normalizeTextValue(rawIssue.code) || `invalid_issue_code_${bucketLabel}_${index}`;
    const issuePath = normalizeTextValue(rawIssue.path) || '$';
    const issueMessage =
      normalizeTextValue(rawIssue.message) ||
      `Issue inválida reportada em '${bucketLabel}' no índice ${index}.`;
    const issueLevel = normalizeTextValue(rawIssue.level).toLowerCase() || level;

    return createIssue(issueLevel, issueCode, issuePath, issueMessage);
  });
};

const summarizeBackendLint = report => ({
  orgs: Array.isArray(report.normalized_orgs) ? report.normalized_orgs.length : 0,
  channels: Array.isArray(report.normalized_channels) ? report.normalized_channels.length : 0,
  nodes: Array.isArray(report.normalized_nodes) ? report.normalized_nodes.length : 0,
  policies: Array.isArray(report.normalized_policies) ? report.normalized_policies.length : 0,
});

const buildNormalizedBlueprintFromBackendReport = report => ({
  schema_name: report.schema_name || '',
  schema_version: report.schema_version || '',
  blueprint_version: report.blueprint_version || '',
  created_at: report.created_at || '',
  updated_at: report.updated_at || '',
  orgs: Array.isArray(report.normalized_orgs) ? report.normalized_orgs : [],
  channels: Array.isArray(report.normalized_channels) ? report.normalized_channels : [],
  nodes: Array.isArray(report.normalized_nodes) ? report.normalized_nodes : [],
  policies: Array.isArray(report.normalized_policies) ? report.normalized_policies : [],
  environment_profile: isPlainObject(report.normalized_environment_profile)
    ? report.normalized_environment_profile
    : {},
  identity_baseline: isPlainObject(report.normalized_identity_baseline)
    ? report.normalized_identity_baseline
    : {},
  fingerprint_sha256: report.fingerprint_sha256 || '',
  schema_runtime: report.schema_runtime || '',
  resolved_schema_version: report.resolved_schema_version || '',
});

export const validateBackendLintContract = rawReport => {
  if (!isPlainObject(rawReport)) {
    return {
      valid: false,
      violations: [
        'Resposta de lint inválida: payload não é objeto no formato esperado (valid/errors/warnings/hints).',
      ],
    };
  }

  const violations = BACKEND_LINT_REQUIRED_FIELDS.filter(field => !(field in rawReport)).map(
    field => `Campo obrigatório ausente no contrato de lint: '${field}'.`
  );

  if (typeof rawReport.valid !== 'boolean') {
    violations.push("Campo 'valid' do contrato de lint deve ser boolean.");
  }

  ['errors', 'warnings', 'hints'].forEach(bucket => {
    if (!Array.isArray(rawReport[bucket])) {
      violations.push(`Campo '${bucket}' do contrato de lint deve ser lista.`);
    }
  });

  return {
    valid: violations.length === 0,
    violations,
  };
};

export const normalizeBackendLintReport = rawReport => {
  const contractValidation = validateBackendLintContract(rawReport);
  if (!contractValidation.valid) {
    return {
      contractValid: false,
      contractViolations: contractValidation.violations,
      report: null,
    };
  }

  const normalizedErrors = ensureIssueList(rawReport.errors, 'error', 'errors');
  const normalizedWarnings = ensureIssueList(rawReport.warnings, 'warning', 'warnings');
  const normalizedHints = ensureIssueList(rawReport.hints, 'hint', 'hints');

  const mergedIssues = sortIssues([...normalizedErrors, ...normalizedWarnings, ...normalizedHints]);
  const mergedErrorIssues = mergedIssues.filter(issue => issue.level === 'error');
  const mergedWarningIssues = mergedIssues.filter(issue => issue.level === 'warning');
  const mergedHintIssues = mergedIssues.filter(issue => issue.level === 'hint');

  const summary = summarizeBackendLint(rawReport);

  const normalizedReport = {
    valid: Boolean(rawReport.valid) && mergedErrorIssues.length === 0,
    schemaVersion: normalizeTextValue(rawReport.schema_version),
    resolvedSchemaVersion:
      normalizeTextValue(rawReport.resolved_schema_version) ||
      normalizeTextValue(rawReport.schema_runtime),
    blueprintVersion: normalizeTextValue(rawReport.blueprint_version),
    createdAt: normalizeTextValue(rawReport.created_at),
    updatedAt: normalizeTextValue(rawReport.updated_at),
    summary,
    errors: mergedErrorIssues,
    warnings: mergedWarningIssues,
    hints: mergedHintIssues,
    issues: mergedIssues,
    fingerprint: normalizeTextValue(rawReport.fingerprint_sha256),
    schemaRuntime: normalizeTextValue(rawReport.schema_runtime),
    currentSchemaVersion: normalizeTextValue(rawReport.current_schema_version),
    lintSource: 'backend',
    contractValid: true,
    normalizedBlueprint: isPlainObject(rawReport.normalized_blueprint)
      ? rawReport.normalized_blueprint
      : buildNormalizedBlueprintFromBackendReport(rawReport),
  };

  if (!normalizedReport.valid && mergedErrorIssues.length === 0) {
    normalizedReport.errors = [
      createIssue(
        'error',
        'lint_backend_invalid_without_errors',
        '$contract.valid',
        'Contrato de lint retornou valid=false sem erros explícitos.'
      ),
    ];
    normalizedReport.issues = sortIssues([
      ...normalizedReport.errors,
      ...normalizedReport.warnings,
      ...normalizedReport.hints,
    ]);
  }

  return {
    contractValid: true,
    contractViolations: [],
    report: normalizedReport,
  };
};

export const buildBlockingLintReport = (
  baseReport,
  {
    code = 'lint_contract_unavailable',
    message = 'Contrato de lint indisponível para validação bloqueante.',
    path = '$contract',
  } = {}
) => {
  const referenceReport = isPlainObject(baseReport)
    ? baseReport
    : {
        valid: false,
        schemaVersion: '',
        resolvedSchemaVersion: BLUEPRINT_RUNTIME_SCHEMA_VERSION,
        blueprintVersion: '',
        createdAt: '',
        updatedAt: '',
        summary: { orgs: 0, channels: 0, nodes: 0, policies: 0 },
        errors: [],
        warnings: [],
        hints: [],
        issues: [],
      };

  const blockingIssue = createIssue('error', code, path, message);
  const mergedIssues = sortIssues([...(referenceReport.issues || []), blockingIssue]);

  return {
    ...referenceReport,
    valid: false,
    errors: mergedIssues.filter(issue => issue.level === 'error'),
    warnings: mergedIssues.filter(issue => issue.level === 'warning'),
    hints: mergedIssues.filter(issue => issue.level === 'hint'),
    issues: mergedIssues,
    lintSource: 'degraded',
    contractValid: false,
  };
};

const DIAGNOSTIC_DOMAIN_ORDER = ['naming', 'topology', 'resources', 'stack', 'contract'];

const DIAGNOSTIC_DOMAIN_META = {
  naming: {
    label: 'Naming',
    matcher: issue =>
      issue.path.includes('domain') ||
      issue.path.includes('org_id') ||
      issue.path.includes('msp') ||
      issue.code.includes('naming') ||
      issue.code.includes('domain') ||
      issue.code.includes('msp'),
  },
  topology: {
    label: 'Topologia',
    matcher: issue =>
      issue.path.includes('channel') ||
      issue.path.includes('orgs') ||
      issue.code.includes('topology') ||
      issue.code.includes('channel') ||
      issue.code.includes('peer') ||
      issue.code.includes('orderer'),
  },
  resources: {
    label: 'Recursos',
    matcher: issue =>
      issue.path.includes('resources') ||
      issue.path.includes('memory') ||
      issue.path.includes('disk') ||
      issue.path.includes('cpu') ||
      issue.code.includes('resource') ||
      issue.code.includes('memory') ||
      issue.code.includes('disk') ||
      issue.code.includes('cpu'),
  },
  stack: {
    label: 'Stack',
    matcher: issue =>
      issue.path.includes('stack') ||
      issue.path.includes('deployment_stack') ||
      issue.code.includes('stack') ||
      issue.code.includes('runtime'),
  },
  contract: {
    label: 'Contrato',
    matcher: issue =>
      issue.path.includes('$contract') ||
      issue.code.includes('contract') ||
      issue.code.includes('backend_unavailable'),
  },
};

export const buildLintDiagnosticDomains = lintReport => {
  if (!isPlainObject(lintReport) || !Array.isArray(lintReport.issues)) {
    return [];
  }

  const counters = DIAGNOSTIC_DOMAIN_ORDER.reduce((accumulator, key) => {
    accumulator[key] = 0;
    return accumulator;
  }, {});

  lintReport.issues.forEach(rawIssue => {
    const issue = {
      code: normalizeTextValue(rawIssue.code).toLowerCase(),
      path: normalizeTextValue(rawIssue.path).toLowerCase(),
    };

    const domainKey = DIAGNOSTIC_DOMAIN_ORDER.find(key =>
      DIAGNOSTIC_DOMAIN_META[key].matcher(issue)
    );

    counters[domainKey || 'contract'] += 1;
  });

  return DIAGNOSTIC_DOMAIN_ORDER.map(key => ({
    key,
    label: DIAGNOSTIC_DOMAIN_META[key].label,
    count: counters[key],
  }));
};

export const buildLintArtifacts = lintReport => {
  const reportPayload = isPlainObject(lintReport)
    ? {
        generated_at_utc: new Date().toISOString().replace(/\.\d{3}Z$/, 'Z'),
        lint_source: lintReport.lintSource || 'unknown',
        contract_valid: Boolean(lintReport.contractValid),
        valid: Boolean(lintReport.valid),
        schema_version: lintReport.schemaVersion || '',
        resolved_schema_version: lintReport.resolvedSchemaVersion || '',
        blueprint_version: lintReport.blueprintVersion || '',
        fingerprint_sha256: lintReport.fingerprint || '',
        created_at: lintReport.createdAt || '',
        updated_at: lintReport.updatedAt || '',
        summary: lintReport.summary || {},
        errors: lintReport.errors || [],
        warnings: lintReport.warnings || [],
        hints: lintReport.hints || [],
        issues: lintReport.issues || [],
      }
    : {};

  const normalizedBlueprintPayload =
    isPlainObject(lintReport) && isPlainObject(lintReport.normalizedBlueprint)
      ? lintReport.normalizedBlueprint
      : {};

  return {
    reportPayload,
    normalizedBlueprintPayload,
  };
};
