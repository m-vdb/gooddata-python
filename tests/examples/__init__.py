import department, worker, salary, forex

hr_examples = [(department, department.Department), (worker, worker.Worker), (salary, salary.Salary)] #dependent on each other, need to be in this order

examples = hr_examples + [(forex, forex.Forex)]
