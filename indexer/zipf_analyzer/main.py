import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.optimize import curve_fit


def zipf_law(rank, C):
    return C / rank


def mandelbrot_law(rank, C, B):
    return C / (rank + B)


def plot_zipf(csv_file, output_prefix="zipf_analysis"):
    print(f"Reading data from {csv_file}...")
    df = pd.read_csv(csv_file)

    ranks = df["rank"].values
    frequencies = df["frequency"].values

    print(f"Total unique terms: {len(ranks)}")
    print(f"Total term occurrences: {sum(frequencies)}")
    print("\nTop 20 terms:")
    print(df.head(20)[["rank", "term", "frequency"]].to_string(index=False))

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

    ax1.loglog(
        ranks, frequencies, "o", markersize=2, alpha=0.5, label="Реальные данные"
    )

    C_ideal = frequencies[0]
    zipf_ideal = zipf_law(ranks, C_ideal)
    ax1.loglog(
        ranks, zipf_ideal, "r-", linewidth=2, label=f"Закон Ципфа (C={C_ideal:.0f})"
    )

    try:
        popt, _ = curve_fit(mandelbrot_law, ranks, frequencies, p0=[C_ideal, 1.0])
        C_mand, B_mand = popt
        mandelbrot_fit = mandelbrot_law(ranks, C_mand, B_mand)
        ax1.loglog(
            ranks,
            mandelbrot_fit,
            "g--",
            linewidth=2,
            label=f"Закон Мандельброта (C={C_mand:.0f}, B={B_mand:.2f})",
        )
        print(f"\nMandelbrot parameters: C={C_mand:.2f}, B={B_mand:.2f}")
    except Exception as e:
        print(f"Warning: Could not fit Mandelbrot law: {e}")

    ax1.set_xlabel("Ранг (log)", fontsize=12)
    ax1.set_ylabel("Частота (log)", fontsize=12)
    ax1.set_title("Закон Ципфа - Логарифмическая шкала", fontsize=14)
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    top_n = min(100, len(ranks))
    ax2.bar(range(1, top_n + 1), frequencies[:top_n], color="steelblue", alpha=0.7)
    ax2.set_xlabel("Ранг", fontsize=12)
    ax2.set_ylabel("Частота", fontsize=12)
    ax2.set_title(f"Топ-{top_n} самых частых термов", fontsize=14)
    ax2.grid(axis="y", alpha=0.3)

    plt.tight_layout()

    filename = f"{output_prefix}.png"
    plt.savefig(filename, dpi=150, bbox_inches="tight")
    print(f"\nGraph saved to: {filename}")

    zipf_error = np.mean(np.abs(frequencies - zipf_ideal) / frequencies) * 100
    print(f"Средняя относительная ошибка закона Ципфа: {zipf_error:.2f}%")

    print("\n=== СТАТИСТИКА ===")
    hapax = sum(1 for f in frequencies if f == 1)
    print(
        f"Hapax legomena (частота=1): {hapax} ({hapax / len(frequencies) * 100:.1f}%)"
    )

    high_freq = sum(1 for f in frequencies if f > 1000)
    print(f"Высокочастотные термы (>1000): {high_freq}")

    medium_freq = sum(1 for f in frequencies if 10 < f <= 1000)
    print(f"Среднечастотные термы (10-1000): {medium_freq}")

    low_freq = sum(1 for f in frequencies if 1 < f <= 10)
    print(f"Низкочастотные термы (2-10): {low_freq}")


def main():
    csv_file = sys.argv[1]
    output_prefix = sys.argv[2] if len(sys.argv) > 2 else "zipf_analysis"

    plot_zipf(csv_file, output_prefix)

    return 0


if __name__ == "__main__":
    sys.exit(main())
