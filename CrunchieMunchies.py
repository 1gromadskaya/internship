import numpy as np

try:
    calorie_stats = np.loadtxt("cereal.csv", delimiter=",")
except OSError:
    calorie_stats = np.loadtxt("./data/cereal.csv", delimiter=",")

average_calories = np.mean(calorie_stats)
print(f"The average number of calories of competitors: {average_calories}")

difference = average_calories - 60
print(f"Difference with CrunchieMunchies: {difference}")

calorie_stats_sorted = np.sort(calorie_stats)
print(calorie_stats_sorted)

median_calories = np.median(calorie_stats)
print(f"Median: {median_calories}")

nth_percentile = 0

for i in range(1, 100):
    value = np.percentile(calorie_stats, i)
    print(f"Percentile {i}%: {value}")

    if value > 60:
        nth_percentile = i
        break

print(f"\nMin percentile > 60 calories: {nth_percentile} percentile")

more_calories = np.mean(calorie_stats > 60) * 100
print(f"ПPercentage of cereal with calorie content > 60: {more_calories:.2f}%")

calorie_std = np.std(calorie_stats)
print(f"Standard deviation: {calorie_std:.2f}")