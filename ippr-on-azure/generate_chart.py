import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

hours = [0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22]
labels = ["12am", "2am", "4am", "6am", "8am", "10am", "12pm", "2pm", "4pm", "6pm", "8pm", "10pm"]

serving  = [2,  2,  2,  2,  3,  4,  4,  4,  3,  2,  2,  2]
training = [3,  3,  3,  2,  1,  1,  1,  1,  1,  2,  3,  3]
batch    = [3,  3,  2,  1,  1,  1,  1,  1,  1,  1,  2,  3]

static_line = [10] * len(hours)

fig, ax = plt.subplots(figsize=(12, 6))

# Shaded region between static line and total IPPR usage
total_ippr = [s + t + b for s, t, b in zip(serving, training, batch)]
ax.fill_between(hours, total_ippr, static_line, alpha=0.3, color='#888888', label='Underutilized with static sizing')

# Static sizing dashed line
ax.plot(hours, static_line, '--', color='#999999', linewidth=2, label='Static sizing (all peaks reserved)')

# Workload lines
ax.plot(hours, serving, 'o-', color='#0078D4', linewidth=2.5, markersize=7, label='Serving (Ray Serve)', zorder=5)
ax.plot(hours, training, 's-', color='#E74C3C', linewidth=2.5, markersize=7, label='Training (Ray Train)', zorder=5)
ax.plot(hours, batch, '^-', color='#2ECC71', linewidth=2.5, markersize=7, label='Batch (Ray Data)', zorder=5)

# Total IPPR line
ax.plot(hours, total_ippr, 'D-', color='#333333', linewidth=1.5, markersize=5, alpha=0.7, label='Total with IPPR', zorder=4)

ax.set_xlabel('Time of Day', fontsize=12, fontweight='bold')
ax.set_ylabel('CPU Cores', fontsize=12, fontweight='bold')
ax.set_title('CPU Allocation by Workload Over 24 Hours', fontsize=14, fontweight='bold', pad=15)

ax.set_xticks(hours)
ax.set_xticklabels(labels, fontsize=10)
ax.set_ylim(0, 14)
ax.set_xlim(-0.5, 22.5)
ax.yaxis.set_major_locator(ticker.MultipleLocator(2))

ax.legend(loc='upper right', fontsize=9, framealpha=0.9)
ax.grid(True, alpha=0.3, linestyle='-')
ax.set_facecolor('#FAFAFA')
fig.patch.set_facecolor('white')

# Annotate the gap
ax.annotate('CPU saved\nwith IPPR', xy=(3, 9), fontsize=10, color='#666666',
            ha='center', style='italic')

plt.tight_layout()
plt.savefig('/Users/alimaazamat/ray/ippr-on-azure/cpu_allocation_chart.png', dpi=150, bbox_inches='tight')
print("Chart saved to ippr-on-azure/cpu_allocation_chart.png")
