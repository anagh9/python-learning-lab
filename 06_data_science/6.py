import pandas as pd


def problem_statement_01(data: dict):
    """
    Top 5 salary by department
    """
    df = pd.DataFrame(data)
    #solution 1
    result = df.groupby("Department")["Salary"].nlargest(5).reset_index()
    print('Solution 1\n', result, '\n')

    
    # solution 2
    df['rank'] = df.groupby('Department')['Salary'].rank(method='first', ascending=False)
    result = df[df['rank'] <= 5].sort_values(['Department', 'rank'])
    print('Solution 2\n', result, '\n')


    #solution 3
    result = df.sort_values(
        ['Department', 'Salary'], ascending=[True, False]
        ).groupby('Department').head(5)
    print('Solution 3\n', result, '\n')
    return result


if __name__ == "__main__":
    data = {
        "emp_name": ["Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Heidi", "Ivan", "Judy", "Ken", "Leo"],
        "Department": ["HR", "HR", "IT", "IT", "Finance", "Finance", "HR", "IT", "Finance", "HR", "IT", "Finance"],
        "Salary": [70000, 75000, 90000, 85000, 80000, 95000, 72000, 88000, 82000, 78000, 92000, 98000]
    }

    problem_statement_01(data)
