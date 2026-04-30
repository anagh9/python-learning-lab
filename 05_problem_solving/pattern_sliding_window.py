"""
Maximum sum subarray of size K 
Classic fixed window. Add first K elements. Then slide: add right element, remove left element, track max. Window size never changes.

arr = [2,1,5,1,3,2], K = 3

Initial window [2,1,5] → sum=8, max=8
Slide → remove 2, add 1 → [1,5,1] sum=7, max=8
Slide → remove 1, add 3 → [5,1,3] sum=9, max=9
Slide → remove 5, add 2 → [1,3,2] sum=6, max=9

Answer: 9
Time O(n)  ·  Space O(1)
"""

def max_sum_subarray(arr, k):
    window_sum = sum(arr[:k])
    max_sum = window_sum
    for i in range(k, len(arr)):
        window_sum += arr[i] - arr[i-k]
        max_sum = max(max_sum, window_sum)
    return max_sum

"""
2. Longest substring without repeating characters
Expand right pointer freely. When a duplicate char enters the window, shrink from left until the duplicate is gone. Track max window size throughout.

s = "abcabcbb"

L=0 R=0 → add 'a' → {a} len=1 max=1
L=0 R=1 → add 'b' → {a,b} len=2 max=2
L=0 R=2 → add 'c' → {a,b,c} len=3 max=3
L=0 R=3 → 'a' exists! shrink: remove s[L]='a' L=1
        → {b,c,a} len=3 max=3
L=1 R=4 → 'b' exists! shrink: remove 'b' L=2 → {c,a,b}
L=2 R=5 → 'c' exists! shrink: remove 'c' L=3 → {a,b,c}
L=3 R=6 → 'b' exists! shrink L=4,5 → {c,b}
L=5 R=7 → 'b' exists! shrink L=6 → {b}

Answer: 3

Time O(n)  ·  Space O(min(n,m)) — m=charset size
"""

def length_of_longest_substring(s):
    char_set = set()
    left = 0
    max_len = 0

    for right in range(len(s)):
        while s[right] in char_set:
            char_set.remove(s[left])
            left+=1
        char_set.add(s[right])
        max_len = max(max_len, right-left + 1)
    return max_len


"""
Fruits into baskets (longest subarray with 2 distinct)

'At most K distinct elements' template. Use a hashmap to count freq. When distinct types exceed K, shrink from left (decrement count, remove if 0). For this problem K=2.

fruits = [1,2,1,2,3]

R=0 → {1:1} distinct=1 window=1
R=1 → {1:1,2:1} distinct=2 window=2
R=2 → {1:2,2:1} distinct=2 window=3 max=3
R=3 → {1:2,2:2} distinct=2 window=4 max=4
R=4 → add 3 → {1:2,2:2,3:1} distinct=3 > 2!
  shrink: remove fruits[L]=1 → {1:1,2:2,3:1} L=1
  still 3 distinct → remove fruits[L]=2 → {1:1,2:1,3:1} L=2
  still 3 → remove fruits[L]=1 → {2:1,3:1} L=3 distinct=2
  window = 4-3+1 = 2

Answer: 4

"""

if __name__ == "__main__":
    assert (max_sum_subarray([2, 1, 5, 1, 3, 2], 3) == 9)
    assert (length_of_longest_substring('abcabcbb') == 3)



"""
"""