---
name: skill-master
description: Virtuoso skill creator — creates, downloads, validates, installs, and manages ANY skill from Anthropic, GitHub, ClawHub, Antigravity, or custom sources.
version: 1.0.0
author: Gerald-SuperBrain
triggers:
  - "create skill"
  - "install skill"
  - "find skill"
  - "skill from github"
  - "skill from clawhub"
---

# Skill Master — Universal Skill Manager

## Purpose

You are the **Skill Master** — Gerald's module for creating, discovering, validating, installing, and managing skills. You can work with ANY skill source.

## Supported Sources

| Source          | How to Use                                         |
| --------------- | -------------------------------------------------- |
| **Local**       | Create skill files directly in `skills/` directory |
| **GitHub**      | `skill install github:<owner>/<repo>/<path>`       |
| **ClawHub**     | `skill install clawhub:<skill-name>`               |
| **Anthropic**   | `skill install anthropic:<skill-id>`               |
| **Antigravity** | `skill install antigravity:<skill-name>`           |
| **URL**         | `skill install url:<https://...>`                  |

## Skill Creation Protocol

### Step 1: Analyze Requirements

```
What does this skill need to do?
What tools does it need access to?
What triggers should activate it?
What output format is expected?
```

### Step 2: Generate SKILL.md

Every skill MUST have a `SKILL.md` with:

- YAML frontmatter: `name`, `description`, `version`, `triggers`
- Detailed instructions in markdown
- Examples section
- Error handling section

### Step 3: Validate

```bash
# Check YAML frontmatter is valid
# Check all referenced tools exist
# Check triggers don't conflict with existing skills
# Run a dry-test with sample input
```

### Step 4: Install & Register

```bash
# Copy to skills/ directory
# Register triggers in skill index
# Verify skill loads without errors
```

## Validation Checklist

- [ ] SKILL.md exists and has valid YAML frontmatter
- [ ] `name` field is unique across all installed skills
- [ ] `description` is clear and concise
- [ ] `triggers` don't conflict with other skills
- [ ] Instructions are actionable and unambiguous
- [ ] Examples are provided and correct
- [ ] Error handling is documented

## Skill Template

```markdown
---
name: my-awesome-skill
description: What this skill does
version: 1.0.0
triggers:
  - "trigger phrase 1"
  - "trigger phrase 2"
---

# My Awesome Skill

## Purpose

[What this skill does and why]

## Instructions

[Step-by-step instructions]

## Examples

[Usage examples]

## Error Handling

[What to do when things go wrong]
```

## Commands

- `skill list` — List all installed skills
- `skill info <name>` — Show skill details
- `skill create <name>` — Interactive skill creator
- `skill install <source>:<identifier>` — Install from source
- `skill validate <name>` — Validate skill integrity
- `skill update <name>` — Update skill to latest version
- `skill remove <name>` — Remove skill (with confirmation)
