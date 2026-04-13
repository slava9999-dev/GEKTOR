# ❄️ [GEKTOR APEX] CHANGE FREEZE PROTOCOL (FROZEN FOUNDATION v1.0)

### 1. IDENTITY OF THE OPERATOR
The Operator is a **Guardian**, not a tinkerer. The Machine is built. The code is closed. Success is measured by adherence to the process, not by the amount of code written during a dull Tuesday.

---

### 2. THE THREE GOLDEN LOCKS (HARD CONSTRAINTS)

#### 🛡️ LOCK 1: SHADOW-STAGING ONLY (THE 48h RULE)
No "improvement" is allowed to touch the live signaling loop directly. 
- All new logic must run in `src/infrastructure/shadow_buffer.py` for a minimum of **48 hours of active market data**.
- If the shadow logic does not demonstrate a >10% improvement in Signal-to-Noise Ratio (SNR) compared to the master version, the branch is discarded.

#### 🛡️ LOCK 2: ANTI-REVENGE COOLDOWN (THE 24h LOCK)
It is strictly forbidden to modify `MathCore`, `VPIN`, or `CUSUM` thresholds within **24 hours after a Stop-Loss execution**. 
- Emotional tuning is the fastest path to curve-fitting. 
- A loss is a statistical sample, not a bug repair request.

#### 🛡️ LOCK 3: STATISTICAL QUORUM (THE 500 SIGNAL RULE)
Mathematical parameters (Thresholds, EMA coefficients) are **FROZEN**. 
- Modifications are only allowed if the `AlphaDecayMonitor` confirms a drop in the Information Coefficient (IC) over a sample of **at least 500 signals**.
- Changes based on "gut feeling" or a single bad day are classified as **Sabotage**.

---

### 3. THE "ZERO-BUDGET" COMPLEXITY RULE
If you add a new parameter, you **must remove an old one**. 
- GEKTOR stays lean. If the system is already "perfect", every addition is a liability.
- Complexity is the entropy that kills HFT systems.

---

### 4. OPERATIONAL DISCIPLINE
- **The "Boredom" Task**: When the market is flat and the system is silent, the Operator's output must be redirected to **Documentation, Infrastructure stability, or Cloud Cost optimization**. 
- **Hands off the Math.**

---

### 5. MERGE PROTOCOL (GIT-SIGNS)
Every PR to `main` must include:
1. `ADR-` (Architecture Decision Record) explaining the **statistical** (not intuitive) reasoning.
2. `SUCCESS_CRITERIA`: A metric that must be met in 1 week, or the change is reverted.
3. `ROLLBACK_HASH`: The commit ID to return to instantly if drift is detected.

**[ABIDE BY THE MATH. TRUST THE RADIUS. PROTECT THE MONOLITH.]**
