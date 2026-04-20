"""
Given a sorted array of integers and a target sum,
return the indices(1-indexed) of the two numbers
that add up to the target.

Example:
Input:  numbers = [2, 7, 11, 15], target = 9
Output: [1, 2](because 2 + 7=9)

Constraints:
- Exactly one solution exists
- Cannot use the same element twice
- Must use O(1) extra space

"""

def two_sum(numbers, target):
    response = [-1,-1]
    mapp = {}

    for index, num in enumerate(numbers):
        if target - num in mapp:
            return [index, mapp[target-num]]

        mapp[num] = index 
    
    return response


# print(two_sum([2, 7, 11, 15], 18))

# If array is sorted use two pointer
def two_sum(numbers, target):
    left, right = 0, len(numbers)-1
    while left< right:
        current_sum = numbers[left] + numbers[right]
        if current_sum == target:
            return [left+1, right+1]
        elif current_sum< target:
            left+=1
        else:
            right -=1
    return [-1,-1]


# print(two_sum([2, 7, 11, 15], 18))


"""
Why this works: Since the array is sorted, if the sum is too small you move left pointer right (increases sum), if too large you move right pointer left (decreases sum). You're guaranteed to find the answer.
"""


# Two pointers = sorted array/string + find a pair + O(1) space constraint

"""
Problem 2:
Given an array height[] where height[i] is the height
of a vertical line at position i, find two lines that
together with the x-axis forms a container that holds
the most water.

Example:
Input:  height = [1, 8, 6, 2, 5, 4, 8, 3, 7]
Output: 49

Water = min(height[left], height[right]) × (right - left)
"""

def vertical_container(heights):
    left, right = 0, len(heights) -1
    max_area = 0
    while left< right:
        h = min(heights[left], heights[right])
        width = right-left
        max_area = max(max_area, h*width)
        if heights[left]<heights[right]:
            left+=1
        else:
            right-=1

    return max_area

# Time: O(n) — one pass
# Space: O(1) — no extra storage

"""
3Sum — find all unique triplets that sum to zero
"""

def three_sum(nums):
    left =0
    right = len(nums) -1
    result = []
    nums.sort()
    for i in range(len(nums)):
        fix = nums[i]
        temp = []
        while left<=right:
            if nums[left] + nums[right] < fix:
                left+=1
            
            if nums[left] + nums[right] + fix== 0:
                temp.append(nums[left])
                temp.append(nums[right])
                temp.append(fix)
                left += 1
                right -= 1
            if nums[left] + nums[right]  > fix:
                right-=1
        if temp:
            result.append(temp)
    return result


if __name__ == "__main__":
    print(three_sum([-1, 0, 1, 2, -1, -4]))

