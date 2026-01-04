import numpy as np
import itertools
import math
import time
from scipy.spatial import distance

np.random.seed(42)

def task_1_change_sign():
    print(f"Task 1")
    arr_random = np.random.randint(0, 11, 10)
    list_input = arr_random.tolist()

    print(f"Input array: {arr_random}")

    res_list = [-x if 3 < x < 8 else x for x in list_input]
    print(f"Python result:  {res_list}")

    arr = arr_random.copy()
    arr[(arr > 3) & (arr < 8)] *= -1
    print(f"Numpy result:   {arr}\n")

def task_2_replace_max():
    print(f"Task 2")

    arr_random = np.random.randint(0, 20, 10)
    list_input = arr_random.tolist()

    print(f"Input array: {arr_random}")
    print(f"Maximum: {arr_random.max()}")

    max_val = max(list_input)
    res_list = [0 if x == max_val else x for x in list_input]
    print(f"Python result:  {res_list}")

    arr = arr_random.copy()
    arr[arr == arr.max()] = 0
    print(f"Numpy result:   {arr}\n")

def task_3_cartesian_product():
    print(f"Task 3")

    arr_random = np.random.randint(0, 10, (3, 2))
    list_input = arr_random.tolist()

    print(f"Input sets:\n{arr_random}")

    res_list = list(itertools.product(*list_input))
    print(f"Python result: {res_list} ")

    arr = arr_random.copy()
    res_arr = np.stack(np.meshgrid(*arr, indexing='ij'), axis=-1).reshape(-1, len(arr))
    print(f"Numpy result:\n{res_arr}\n")

def task_4_match_rows():
    print(f"Task 4")

    B_arr = np.random.randint(0, 10, (2, 2))
    A_arr = np.random.randint(0, 10, (8, 3))

    list_A = A_arr.tolist()
    list_B = B_arr.tolist()

    print("Array B:\n", B_arr)
    print("Array A:\n", A_arr[:8])

    res_list = [
        row_a for row_a in list_A
        if all(any(x in row_a for x in row_b) for row_b in list_B)
    ]
    print(f"Python result: {res_list}")

    mask = np.logical_and.reduce([np.isin(A_arr, row).any(axis=1) for row in B_arr])
    res_arr = A_arr[mask]
    print(f"Numpy result:\n{res_arr}\n")

def task_5_unequal_rows():
    print(f"Task 5")

    arr_random = np.random.randint(1, 4, (10, 3))

    list_input = arr_random.tolist()
    print(f"Input (первые 4 строки):\n{arr_random[:10]}")

    res_list = [row for row in list_input if len(set(row)) > 1]
    print(f"Python result: {res_list}")

    arr = arr_random.copy()
    res_arr = arr[arr.min(axis=1) != arr.max(axis=1)]
    print(f"Numpy result: {res_arr}\n")

def task_6_remove_duplicates():
    print(f"Task 6")

    arr_random = np.random.randint(0, 5, (5, 3))

    list_input = arr_random.tolist()
    print(f"Input (in total {len(arr_random)} rows):\n{arr_random}")

    res_list = sorted([list(x) for x in set(tuple(x) for x in list_input)])
    print(f"Python result:\n{res_list}")

    arr = arr_random.copy()
    res_arr = np.unique(arr, axis=0)
    print(f"Numpy result:\n{res_arr}\n")

def task_1_diagonal_product():
    print(f"Task 1.1")

    arr_random = np.random.randint(0, 4, (4, 5))
    list_input = arr_random.tolist()

    print(f"Matrix:\n{arr_random}")

    min_dim = min(len(list_input), len(list_input[0]))
    diag_elements = [list_input[i][i] for i in range(min_dim) if list_input[i][i] != 0]

    prod_py = 1
    if not diag_elements:
        prod_py = 0
    else:
        for x in diag_elements:
            prod_py *= x

    print(f"Python result: {prod_py}")

    arr = arr_random.copy()
    diag = np.diag(arr)
    res_np = np.prod(diag[diag != 0])
    print(f"Numpy result:  {res_np}\n")

def task_2_multiset_check():
    print(f"Task 2.1")

    x_arr = np.random.randint(0, 5, 6)
    y_arr = np.random.randint(0, 5, 6)

    list_x = x_arr.tolist()
    list_y = y_arr.tolist()

    print(f"x: {x_arr}")
    print(f"y: {y_arr}")

    res_py = sorted(list_x) == sorted(list_y)
    print(f"Python result: {res_py}")

    res_np = np.array_equal(np.sort(x_arr), np.sort(y_arr))
    print(f"Numpy result:  {res_np}\n")

def task_3_max_after_zero():
    print(f"Task 3.1")

    arr_random = np.random.randint(0, 10, 20)

    list_input = arr_random.tolist()
    print(f"Array: {arr_random}")

    candidates = [list_input[i] for i in range(1, len(list_input)) if list_input[i - 1] == 0]
    res_py = max(candidates) if candidates else None
    print(f"Python result: {res_py}")

    arr = arr_random.copy()
    zero_mask = (arr[:-1] == 0)
    target_elements = arr[1:][zero_mask]

    res_np = target_elements.max() if target_elements.size > 0 else None
    print(f"Numpy result:  {res_np}\n")

def task_4_rle():
    print(f"Task 4.1")

    arr_random = np.random.randint(0, 3, 15)

    list_input = arr_random.tolist()
    print(f"Input: {arr_random}")

    py_vals = []
    py_counts = []
    for k, g in itertools.groupby(list_input):
        py_vals.append(k)
        py_counts.append(len(list(g)))
    print(f"Python result: Values={py_vals}, Counts={py_counts}")

    arr = arr_random.copy()
    change_indices = np.where(arr[:-1] != arr[1:])[0] + 1
    split_indices = np.concatenate(([0], change_indices, [len(arr)]))

    counts = np.diff(split_indices)
    values = arr[split_indices[:-1]]
    print(f"Numpy result:  Values={values}, Counts={counts}\n")

def task_5_euclidean_dist():
    print(f"Task 5.1")

    X = np.random.random((100, 50))
    Y = np.random.random((200, 50))

    print(f"Shape X: {X.shape}, Shape Y: {Y.shape}")

    start_py = time.time()
    X_list = X.tolist()
    Y_list = Y.tolist()
    dist_py = []
    for row_x in X_list:
        row_dists = []
        for row_y in Y_list:
            sq_diff = sum((px - py) ** 2 for px, py in zip(row_x, row_y))
            row_dists.append(math.sqrt(sq_diff))
        dist_py.append(row_dists)

    end_py = time.time()
    print(f"Python time: {end_py - start_py:.5f} sec")

    start_np = time.time()
    dists_np = np.sqrt(np.sum((X[:, np.newaxis, :] - Y[np.newaxis, :, :]) ** 2, axis=2))
    print(f"Numpy time:  {time.time() - start_np:.5f} sec")

    start_sci = time.time()
    dists_scipy = distance.cdist(X, Y, 'euclidean')
    print(f"Scipy time:  {time.time() - start_sci:.5f} sec")

    match = np.allclose(dists_np, dists_scipy)
    print(f"Match: {match}\n")

if __name__ == "__main__":
    task_1_change_sign()
    task_2_replace_max()
    task_3_cartesian_product()
    task_4_match_rows()
    task_5_unequal_rows()
    task_6_remove_duplicates()
    task_1_diagonal_product()
    task_2_multiset_check()
    task_3_max_after_zero()
    task_4_rle()
    task_5_euclidean_dist()
