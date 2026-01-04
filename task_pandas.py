import pandas as pd
import io

data = pd.read_csv("./data/adult.data.csv")

print("Number of male and female:")
sex_counts = data['sex'].value_counts()
print(sex_counts)

average_age_men = data[data['sex'] == 'Male']['age'].mean()
print(f"\nAverage men age: {round(average_age_men, 2)}")

us_fraction = (data['native-country'] == 'United-States').mean()
print(f"\nAverage of USA native country: {us_fraction:.3f}")
print(f"In percents: {us_fraction * 100:.2f}%\n")

high_salary_ages = data[data['salary'] == '>50K']['age']
mean_high = high_salary_ages.mean()
std_high = high_salary_ages.std()

print(f"For those earning >50K average age: {mean_high:.2f} years")
print(f"Standard deviation: {std_high:.2f} years")

low_salary_ages = data[data['salary'] == '<=50K']['age']
mean_low = low_salary_ages.mean()
std_low = low_salary_ages.std()

print(f"For those earning <=50K average age: {mean_low:.2f} years")
print(f"Standard deviation: {std_low:.2f} years\n")

high_ed_levels = ['Bachelors', 'Prof-school', 'Assoc-acdm', 'Assoc-voc', 'Masters', 'Doctorate']
high_earners = data[data['salary'] == '>50K']
non_high_ed_rich = high_earners[~high_earners['education'].isin(high_ed_levels)]

if len(non_high_ed_rich) > 0:
    print("People who earn more than 50k do not have a university degree")
    print("People earning >50K with other education levels:")
    print(non_high_ed_rich['education'].value_counts())
else:
    print("People who earn more than 50k have at least a university degree\n")

stats = data.groupby(['race', 'sex'])['age'].describe()
max_age = stats.loc[('Asian-Pac-Islander', 'Male'), 'max']
print(f"\nThe maximum age of men of the race Asian-Pac-Islander: {max_age}\n")

men_data = data[data['sex'] == 'Male'].copy()
men_data['is_married'] = men_data['marital-status'].str.startswith('Married')
married_men = men_data[men_data['is_married']]
single_men = men_data[~men_data['is_married']]
married_share = (married_men['salary'] == '>50K').mean()
single_share = (single_men['salary'] == '>50K').mean()

print(f"Proportion of high earners among married men: {married_share:.2%}")
print(f"Proportion of high earners among single men: {single_share:.2%}")

if married_share > single_share:
    print("Conclusion: The share of high earners is higher among married men.")
else:
    print("Conclusion: The share of high earners is higher among single men.")

max_load = data['hours-per-week'].max()
num_workaholics_data = data[data['hours-per-week'] == max_load]
num_workaholics = num_workaholics_data.shape[0]
rich_percentage = (num_workaholics_data['salary'] == '>50K').mean() * 100

print(f"\nMax time - {max_load} hours/week.")
print(f"Total number of such hard workers {num_workaholics}")
print(f"Percentage of rich among them {rich_percentage:.0f}%")

avg_hours = data.groupby(['native-country', 'salary'])['hours-per-week'].mean()
print("Average hours per week (Grouped List):")
print(avg_hours)

print("\nAverage hours per week (Table Format):")
print(avg_hours.unstack())

bins = [16, 35, 70, 100]
labels = ['young', 'adult', 'retiree']
data['AgeGroup'] = pd.cut(data['age'], bins=bins, labels=labels, include_lowest=True)
print(data[['age', 'AgeGroup']].head(10))

high_earners = data[data['salary'] == '>50K']
count_by_group = high_earners.groupby('AgeGroup', observed=False)['salary'].count()
print("Number of people earning >50K by group:")
print(count_by_group)

proportions = data.groupby('AgeGroup', observed=False)['salary'].apply(lambda x: (x == '>50K').mean())
print("\nProportion of >50K earners by group:")
print(proportions)
most_frequent_group = proportions.idxmax()
print(f"The group where people most often earn >50K is: {most_frequent_group}")

occupation_counts = data.groupby('occupation').size()
print("Number of people in each occupation group:")
print(occupation_counts)

def filter_func(group):
    mean_age_check = group['age'].mean() <= 40
    hours_check = (group['hours-per-week'] > 5).all()
    return mean_age_check and hours_check

filtered_data = data.groupby('occupation').filter(filter_func)

print("\nOccupations that passed the filter:")
print(filtered_data['occupation'].unique())

print("First few rows of the filtered dataset:")
print(filtered_data.head())