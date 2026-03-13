
def fibonacci(n):
    if n <= 1:
        return n
    return fibonacci(n-1) + fibonacci(n-2)

def main():
    result = fibonacci(8)
    print(f'Fibonacci of 8 is {result}')

if __name__ == '__main__':
    main()
