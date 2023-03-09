def test(a):
    a.append(0)

if __name__ == '__main__':
    a = [1,2,3]
    print(a)
    test(a)
    print(a)