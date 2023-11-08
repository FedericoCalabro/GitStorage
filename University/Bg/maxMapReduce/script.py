from random import randint

numbers = [f"{str(randint(0,100000000000))}\n" for i in range(25000000//3)]

with open('input3.txt', 'w') as file:
    file.writelines(numbers)
