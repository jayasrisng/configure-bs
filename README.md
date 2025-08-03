# 🧠 configure-bs – Privacy-Preserving VR Telemetry with WGAN-GP

This project addresses user privacy in immersive VR by generating synthetic Beat Saber telemetry using WGAN-GP combined with Differential Privacy (DP-SGD). It enables secure analytics without compromising user identity or behavior patterns.

---

## 🎮 Project Overview

In immersive VR systems like Beat Saber, motion telemetry is rich but personally identifiable. This project uses deep generative modeling (WGAN-GP) and differential privacy to anonymize user traces, enabling safe research, game design, and analytics.

We simulate realistic, high-resolution motion data that preserves utility (for skill measurement, UX feedback, etc.) while preventing re-identification attacks.

---

## 🧰 Tools & Technologies

- **Python** – Data pipelines and scripting
- **TensorFlow** – WGAN-GP implementation and model training
- **TensorFlow Privacy** – Differential Privacy using DP-SGD
- **Scikit-learn** – Preprocessing and metrics
- **Matplotlib / Seaborn** – Visualization
- **Unity3D + SteamVR** – Telemetry data collection

---

## 🔄 Step-by-Step Workflow

### 1. Data Collection & Preprocessing
- 2,598 Beat Saber sessions captured via Unity + SteamVR
- Features: saber direction, speed, angular velocity, hit accuracy, time deltas
- Data cleaned, normalized (Min-Max), and split for training

### 2. Baseline Privacy Risk
- Cosine similarity re-ID attack: 100% match rate on raw data

### 3. Synthetic Data Generation
- WGAN-GP model with fully connected layers and gradient penalty
- Preserved motion dynamics across beat intervals

### 4. Privacy Enhancement
- DP-SGD added to discriminator
- Tuned epsilon, noise multiplier, clipping norm
- Reduced re-ID accuracy from 100% → **0.04%**

### 5. Evaluation & Visualization
- KDE and KS tests (p > 0.05) → synthetic data statistically similar
- Visual graphs confirm feature preservation without identity leakage

---

## 🔐 Encrypted Analytics (Optional)
Includes a CKKS-based homomorphic encryption prototype (`homomorphic.ipynb`) to run/test secure analysis on synthetic data post-generation.

---

## 🏭 Productionization Strategy

- API microservice for pre-storage anonymization
- PyTorch GPU batch deployment (Docker-ready)
- Unity-compatible: replace raw logs with synthetic telemetry

---

## 🌍 Real-World Use Cases

- **VR Research Labs** – Share anonymized motion datasets
- **Game Design** – Test level difficulty using synthetic profiles
- **Privacy-First Analytics** – GDPR-compliant telemetry pipelines
- **Skill Feedback** – Build secure esports or training benchmarks

---

## 📂 Key Files

- `configure bs.ipynb` – Pipeline walkthrough
- `data processing.ipynb` – Telemetry wrangling
- `WGAN-GP.ipynb` – GAN training and synthesis
- `homomorphic.ipynb` – Encrypted analysis
- `*.csv` – Skill ratings, mappings, and generated data
- `saved_generator_model.keras` – Trained model

---

## 🧪 Sample Outputs

- `synthetic_user_data.csv`  
- `user_skill_ratings_ranked_rounded.csv`  
- `user_skill_feature_mapping.csv`

---

## 📥 How to Use

### 1. ⭐ Star this repo if it helped you!

### 2. 🍴 Fork instead of downloading

Please **fork** this repo instead of just downloading and re-uploading it. Here’s how:

- Click the `Fork` button at the top right of the page
- Clone your fork:

```bash
git clone https://github.com/YOUR-USERNAME/configure-bs.git
cd configure-bs
```

### 3. 🧪 Run the Pipeline

Make sure you have Python 3.8+ and install dependencies:

```bash
pip install -r requirements.txt
```

Open notebooks in Jupyter or VSCode:

```bash
jupyter notebook
```

Suggested order:
1. `configure bs.ipynb`
2. `data processing.ipynb`
3. `WGAN-GP.ipynb`
4. `homomorphic.ipynb`

---

## 🔐 VR + Privacy: Concept

This project is based on the idea that privacy and experimentation should coexist.

> In XR systems, telemetry data is rich but sensitive. Our pipeline enables behavioral analysis and synthetic replay without exposing real users. Encrypted analytics further protect downstream computation—making this suitable for use in research, publishing, or collaborative environments.

---

## 🧪 Sample Outputs

- `synthetic_user_data.csv` – WGAN-GP generated data
- `user_skill_ratings_ranked_rounded.csv` – Cleaned ratings
- `user_skill_feature_mapping.csv` – Input-to-output mapping

---

## 🛠 Tech Stack

- Python, NumPy, pandas, matplotlib
- TensorFlow / Keras
- Jupyter Notebooks
- Homomorphic Encryption (CKKS via TenSEAL / Microsoft SEAL)
- GAN training with gradient penalty (WGAN-GP)

---

## 👤 Author

Created by [Jayasri](https://github.com/jayasrisng)

If you use this work in academic or applied settings, please cite:
J. S. N. Guthula, H. Rashid, J. P. Springer and A. Basu, "Preserving Privacy in VR Telemetry Data," 2025 IEEE Conference on Virtual Reality and 3D User Interfaces Abstracts and Workshops (VRW), Saint Malo, France, 2025, pp. 1270–1271, doi: 10.1109/VRW66409.2025.00281.

---

## 📄 License

MIT License – free to use, modify, and build upon with attribution.
