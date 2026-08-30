# Build a Codex Harness for This Existing Project

I already have an existing project repository with some initial files, folders, configuration, and possibly partial implementation.

Before implementing any new project features, I want you to build a **minimal but practical Codex harness** around the existing repository so future Codex sessions can understand the project, follow its conventions, validate their work, and improve through feedback loops.

## Primary Goal

Create **Harness v1** for this repository.

The harness should help future Codex agents:

1. understand the project quickly;
2. discover the relevant architecture and documentation;
3. follow project-specific engineering conventions;
4. reuse repeatable workflows through Skills;
5. validate changes automatically;
6. detect common architectural or quality violations;
7. review their own changes before finishing.

Do NOT implement the actual project features unless required only to make the harness itself functional.

---

# Step 1 — Inspect the Existing Repository

Start by thoroughly inspecting the repository.

Identify:

* current directory structure;
* programming languages;
* frameworks and libraries;
* package/dependency management;
* existing source folders;
* existing tests;
* existing scripts;
* linting/formatting tools;
* CI configuration;
* Docker configuration;
* architecture-related files;
* README/documentation;
* environment/config files;
* existing `AGENTS.md`;
* existing `.agents/skills`;
* existing conventions that can be inferred from the code.

Do not assume the architecture before inspecting the repository.

Preserve existing conventions whenever they are reasonable.

Do NOT rename, delete, or reorganize existing project files unnecessarily.

Before making changes, form a short internal model of:

* what this project appears to do;
* how the repository is currently structured;
* what is already available;
* what the harness is currently missing.

---

# Step 2 — Build `AGENTS.md`

Create or improve the repository-level:

`AGENTS.md`

Keep it concise.

`AGENTS.md` should act as a **navigation map for Codex**, not as a giant documentation file.

It should include:

## Project Overview

Briefly describe the project based on the existing repository.

## Repository Structure

Explain the important folders.

## Source of Truth

Point Codex to the appropriate documentation files.

For example:

* architecture;
* engineering conventions;
* data contracts;
* testing;
* important design decisions.

## Important Engineering Invariants

Only include rules that are genuinely relevant to this repository.

Examples could include:

* layer responsibilities;
* module boundaries;
* allowed dependency directions;
* data validation requirements;
* test requirements;
* naming conventions;
* configuration handling;
* error handling;
* logging conventions.

Infer these from the repository where possible.

If an invariant cannot currently be determined, do not invent one.

## Development Workflow

Explain what future Codex agents should do before completing a task.

The expected general loop should be similar to:

1. inspect relevant code;
2. read relevant documentation;
3. use relevant Skill if available;
4. implement the smallest reasonable change;
5. run tests;
6. run lint/static checks;
7. run project validation;
8. inspect the diff;
9. fix issues;
10. rerun validation before finishing.

## Commands

Document the actual commands supported by this repository for:

* tests;
* lint;
* formatting;
* type checking;
* project validation.

Only document commands that actually work.

---

# Step 3 — Create a Lightweight Documentation Structure

Create a minimal `docs/` structure only where useful.

Prefer something like:

```text
docs/
├── architecture.md
├── engineering-conventions.md
├── testing.md
└── decisions/
```

Adapt this to the existing project instead of blindly creating every file.

## `architecture.md`

Document the architecture that can already be inferred from the repository.

Include:

* major components;
* responsibilities;
* dependency flow;
* data flow if applicable;
* boundaries between modules/layers.

Do not invent future architecture unless clearly marked as a recommendation.

## `engineering-conventions.md`

Document conventions visible in the repository such as:

* naming;
* module organization;
* configuration;
* logging;
* exception handling;
* data access patterns;
* API patterns;
* testing expectations.

## `testing.md`

Explain:

* types of tests currently supported;
* where tests belong;
* how to run them;
* what future changes are expected to test.

## `docs/decisions/`

Only create this if the project already has meaningful architectural decisions worth recording.

Do not create unnecessary ADRs just to populate the folder.

---

# Step 4 — Add Repository Skills

Create repository-local Codex Skills under:

```text
.agents/skills/
```

Do NOT create many Skills.

Start with only **2–3 high-value Skills** based on workflows that are likely to repeat in this repository.

Each Skill should have a clear trigger description and a focused workflow.

Possible examples:

```text
.agents/skills/
├── implement-feature/
│   └── SKILL.md
├── project-validation/
│   └── SKILL.md
└── code-review/
    └── SKILL.md
```

But rename or replace them when the repository suggests more appropriate workflows.

For example, if this is a Data Engineering repository, more appropriate Skills might be:

* ingestion pipeline;
* transformation pipeline;
* data quality;
* pipeline review.

If this is an API project:

* create endpoint;
* service implementation;
* API testing.

If this is a frontend project:

* component implementation;
* UI test;
* accessibility review.

Do not force generic Skills when project-specific Skills are more useful.

Each `SKILL.md` should:

* have proper metadata;
* clearly state when it should trigger;
* reference repository documentation rather than duplicating it;
* define a repeatable workflow;
* tell Codex what validation should run before completion.

Avoid putting large amounts of static project knowledge inside Skills.

Project knowledge belongs primarily in `docs/`.

---

# Step 5 — Build an Executable Validation Layer

This is the most important part of the harness.

Do not rely only on written instructions.

Create an executable validation entry point such as:

```text
scripts/validate_project.py
```

or reuse/improve an existing validation script.

The validation layer should orchestrate checks that already make sense for this project.

Examples:

* tests;
* lint;
* formatting checks;
* type checking;
* import validation;
* architecture rules;
* configuration validation;
* schema validation;
* dependency-boundary checks.

Prefer reusing existing tools instead of introducing unnecessary dependencies.

The final goal should be that future Codex agents can run one clear command such as:

```bash
python scripts/validate_project.py
```

and receive useful feedback.

If this repository is better suited for another command, use the appropriate approach.

---

# Step 6 — Add High-Value Architectural Checks

Identify only a small number of rules that:

1. are important;
2. can be verified automatically;
3. are likely for an AI coding agent to accidentally violate.

If such rules exist, add checks for them.

Examples:

* forbidden dependency directions;
* prohibited imports;
* source modules that must not depend on higher layers;
* required test placement;
* schema rules;
* configuration rules;
* naming rules.

Do not create arbitrary architecture rules.

Each validation failure should contain an **agent-friendly remediation message**.

Bad:

```text
ERROR 104
```

Good:

```text
Architecture validation failed:

The domain layer must not import infrastructure modules.

Move the infrastructure dependency behind an interface.

See:
docs/architecture.md#dependency-direction
```

Validation errors should help Codex understand how to repair the problem.

---

# Step 7 — Tests

Inspect existing tests before adding anything.

Do not create meaningless tests purely to increase coverage.

If the harness itself introduces validation utilities, add focused tests for them when appropriate.

Future Codex changes should be able to use the existing test infrastructure.

If no test framework exists yet but adding one is clearly appropriate, introduce the minimum necessary setup.

Avoid unnecessary dependencies.

---

# Step 8 — CI

Inspect whether CI already exists.

If CI already exists:

Integrate project validation into it when reasonable.

For example:

```text
test
↓
lint
↓
validate project
```

If CI does not exist:

Do NOT automatically build a large CI/CD system.

Only create a minimal CI workflow if it provides immediate value and requires little configuration.

Otherwise document how validation should later be connected to CI.

---

# Step 9 — Do Not Overengineer

This should be **Harness v1**, not a complete enterprise platform.

Avoid:

* huge `AGENTS.md`;
* dozens of Skills;
* complicated hooks;
* unnecessary MCP integrations;
* unnecessary new dependencies;
* premature CI/CD complexity;
* large architecture frameworks;
* excessive custom linters;
* rewriting working project code.

Prefer:

```text
small
+
clear
+
executable
+
easy to extend
```

The harness should evolve when Codex repeatedly encounters real problems.

---

# Step 10 — Verify the Harness

After implementing the harness:

Run all available checks.

At minimum attempt:

* tests;
* lint;
* formatting checks if configured;
* type checking if configured;
* project validation.

Fix any harness-related failures.

Then inspect the final git diff.

Make sure you have not accidentally changed unrelated project behavior.

---

# Desired Final Repository Shape

Do not force this exact structure, but aim for something conceptually similar:

```text
project/
│
├── AGENTS.md
│
├── README.md
│
├── docs/
│   ├── architecture.md
│   ├── engineering-conventions.md
│   └── testing.md
│
├── .agents/
│   └── skills/
│       ├── <high-value-skill-1>/
│       │   └── SKILL.md
│       └── <high-value-skill-2>/
│           └── SKILL.md
│
├── src/ or existing source structure
│
├── tests/
│
├── scripts/
│   └── validate_project.*
│
└── existing project configuration
```

Adapt everything to the repository that actually exists.

---

# Important Constraint

Do NOT start implementing planned product/business features.

This task is specifically:

> Understand the existing repository and create the engineering harness that future Codex agents will use while implementing the project.

Only modify application code when absolutely necessary to make the harness or existing validation infrastructure work.

---

# Final Response

When finished, give me:

## 1. Repository Assessment

Briefly explain what you discovered about the project.

## 2. Harness Added

List the files created or modified and their purpose.

## 3. Codex Workflow

Explain how a future Codex session should now operate.

For example:

```text
User task
↓
AGENTS.md
↓
relevant docs
↓
relevant Skill
↓
implementation
↓
tests
↓
validation
↓
diff review
↓
fix
↓
final validation
```

## 4. Validation Commands

Show the exact commands I should run manually.

## 5. Current Limitations

Explain what the harness cannot yet verify automatically.

## 6. Recommended Harness v2 Improvements

Only recommend improvements justified by real limitations found in the repository.

Do not implement Harness v2 yet.
