import os
import argparse


class SkillForge:
    """Professional Skill Scaffolding Engine for Gerald-SuperBrain."""

    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    TEMPLATE = """---
name: {name}
description: {description}
version: 1.0.0
author: Gerald-SuperBrain
triggers:
  - "{trigger}"
---

# {title}

## 🎯 Purpose
{description}

## 🚦 Workflow
1. **Analyze**: Understand the input.
2. **Execute**: Perform the core task using appropriate tools.
3. **Verify**: Ensure the output matches requirements.

## 🛡 Constraints
- Always use absolute paths.
- Handle errors gracefully.

## 🧪 Examples
- User: "Help me with {name}" -> Action: [Describe action]
"""

    def __init__(self, name: str):
        self.name = name.lower().replace(" ", "-")
        self.target_dir = os.path.join(self.BASE_DIR, self.name)

    def forge(self, description: str, trigger: str):
        if not os.path.exists(self.target_dir):
            os.makedirs(self.target_dir)
            print(f"[*] Created directory: {self.target_dir}")

        filepath = os.path.join(self.target_dir, "SKILL.md")
        content = self.TEMPLATE.format(
            name=self.name,
            description=description,
            trigger=trigger,
            title=self.name.replace("-", " ").title(),
        )

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(content)

        print(f"[+] Skill '{self.name}' successfully forged at {filepath}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gerald Skill Forge")
    parser.add_argument("name", help="Name of the skill")
    parser.add_argument("--desc", required=True, help="Skill description")
    parser.add_argument("--trigger", required=True, help="Main trigger phrase")

    args = parser.parse_args()
    forge = SkillForge(args.name)
    forge.forge(args.desc, args.trigger)
