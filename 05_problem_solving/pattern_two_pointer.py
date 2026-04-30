"""
Two pointers = sorted array/string + find a pair + O(1) space constraint
Whenever you see those 3 signals together, reach for two pointers before a hashmap.

Signal in problem                                   Think
--------------------------------------------------------------------------
Sorted array + find pair + O(1) space               Two pointers
Find max/min area or distance b/w elements end      Two pointers from both Bottleneck logic (min of two things)                Move the shorter pointer
"""

"""
Given a sorted array of integers and a target sum,
return the indices (1-indexed) of the two numbers
that add up to the target.

Example:
Input:  numbers = [2, 7, 11, 15], target = 9
Output: [1, 2]   (because 2 + 7 = 9)

Constraints:
- Exactly one solution exists
- Cannot use the same element twice
- Must use O(1) extra space
"""


def two_sum(nums, target):
    """
    TC: O(n)
    Space: O(1)
    """
    left, right = 0, len(nums) - 1
    while left < right:
        current_sum = nums[left] + nums[right]
        if current_sum == target:
            return [left+1, right+1]  # 1- indexed
        elif current_sum < target:
            left += 1
        else:
            right -= 1
    return [-1, -1]


"""
Problem 2: Given an array height[] where height[i] is the height
of a vertical line at position i, find two lines that
together with the x-axis forms a container that holds
the most water.

Example:
Input:  height = [1, 8, 6, 2, 5, 4, 8, 3, 7]
Output: 49

Water = min(height[left], height[right]) × (right - left)
"""


def max_water(height):
    # Time: O(n) — one pass
    # Space: O(1) — no extra storage

    left, right = 0, len(height) - 1
    max_area = 0

    while left < right:
        h = min(height[left], height[right])
        width = right-left
        max_area = max(max_area, h * width)

        if height[left] < height[right]:
            left += 1
        else:
            right -= 1
    return max_area


"""
3Sum — find all unique triplets that sum to zero
Fix one number with a loop. On the remaining subarray (which you sort), run two pointers to find a pair that sums to -fixed. This turns an O(n³) brute force into O(n²).

sorted → [-4, -1, -1, 0, 1, 2]
Result: [[-1,-1,2], [-1,0,1]]
"""


def three_sums(nums):
    nums.sort()
    result = []

    for i in range(len(nums)-2):
        if i > 0 and nums[i] == nums[i-1]:
            continue  # skiped duplicate fixed value
        left, right = i+1, len(nums)-1
        while left < right:
            s = nums[i]+nums[left] + nums[right]
            if s == 0:
                result.append([nums[i], nums[left], nums[right]])
                while left < right and nums[left] == nums[left+1]:
                    left += 1
                while left < right and nums[right] == nums[right-1]:
                    right -= 1
                left += 1
                right -= 1
            elif s < 0:
                left += 1
            else:
                right -= 1
    return result


"""
Remove duplicates from sorted array — in-place
i/p : [1, 1, 2, 3, 3, 4]
o/p : 4
"""


def remove_duplicates(nums):
    # Time O(n) · Space O(1)
    if not nums:
        return 0
    slow = 1
    for fast in range(1, len(nums)):
        if nums[fast] != nums[fast-1]:
            nums[slow] = nums[fast]
            slow += 1
    # print(nums[:slow]) # For removed List
    return slow


"""
Valid palindrome — ignoring non-alphanumeric chars
 "A man, a plan, a canal: Panama"

cleaned thinking: "amanaplanacanalpanama"

L=0(A) R=last(a) → match (case-insensitive) → L++ R--
L=1(m) R=prev(m) → match → L++ R--
... (skip commas and spaces as we go)
... all chars match
L >= R → return True ✓

"""


def is_palindrome(s):
    # Time O(n) · Space O(1)

    left, right = 0, len(s) - 1

    while left < right:
        # skip non-alphanumeric from left
        while left < right and not s[left].isalnum():
            left += 1
        # skip non-alphanumeric from right
        while left < right and not s[right].isalnum():
            right -= 1

        if s[left].lower() != s[right].lower():
            return False

        left += 1
        right -= 1

    return True


"""
Trapping rain water — hardest two pointer problem

Water at any position = min(max_left, max_right) - height[i]. Instead of precomputing both arrays, use two pointers: whichever side has the smaller max determines the water at that position — process it and move inward.

Dry run — [0, 1, 0, 2, 1, 0, 1, 3, 1, 0, 1, 2]
left=0 right=11, max_L=0 max_R=0, water=0

height[L]=0, height[R]=2
max_L=0, max_R=2 → max_L < max_R → process left
  water += max(0, max_L - height[L]) = 0 → L++

L=1(h=1): max_L=1, max_R=2 → process left
  water += max(0, 1-1)=0 → L++

L=2(h=0): max_L=1, max_R=2 → process left
  water += max(0, 1-0)=1 → L++ [total=1]

L=3(h=2): max_L=2 → water+=max(0,2-2)=0 → L++
...continue until L>=R

Final water trapped = 6
"""


def trap(height):
    left, right = 0, len(height) - 1
    max_left = max_right = water = 0

    while left < right:
        if height[left] < height[right]:
            if height[left] >= max_left:
                max_left = height[left]
            else:
                water += max_left - height[left]
            left += 1
        else:
            if height[right] >= max_right:
                max_right = height[right]
            else:
                water += max_right - height[right]
            right -= 1

    return water


if __name__ == "__main__":
    assert (max_water([1, 8, 6, 2, 5, 4, 8, 3, 7]) == 49)
    assert (three_sums([-4, -1, -1, 0, 1, 2]) == [[-1, -1, 2], [-1, 0, 1]])
    assert (remove_duplicates([1, 1, 2, 3, 3, 4]) == 4)
    assert (is_palindrome("A man, a plan, a canal: Panama") == True)
    assert (is_palindrome("amanaplanacanalpanama") == True)
    assert (trap([0, 1, 0, 2, 1, 0, 1, 3, 1, 0, 1, 2]) == 6)
