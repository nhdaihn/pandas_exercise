# coding=utf-8
import pandas as pd


# sort (and reset index) prices dataframe by product_id & updated_at
def sort_prices_df(data_frame):
    result = data_frame.sort_values(by=['product_id', 'updated_at']).reset_index(drop=True)
    return result


# get first row of each group prices dataframe
# filter to find "giá gốc :v" rồi dựa vào kq này để thêm các row 1/1/1900 0: 00
def filter_prices_df(data_frame):
    result = data_frame.groupby('product_id').first().reset_index()
    return result


# read prices.csv
prices_df = pd.read_csv(r'prices.csv')

# sort prices_df
sort_result = sort_prices_df(prices_df)

# filter sort_result
filter_result = filter_prices_df(sort_result)

# for lặp tất cả các phần tử của dataframe vừa filter
for i in range(len(filter_result)):
    # định nghĩa 1 row mới có các colums là product_id, old_price...
    new_row = {'product_id': filter_result.loc[i, "product_id"],
               'old_price': 'null',  # h
               'new_price': filter_result.loc[i, "old_price"],
               'updated_at': '1/1/1900 0:00'  # h
               }
    # thêm row này vào dataframe. Chèn mấy cái row 1/1/1900 0:00 đấy đấy c
    prices_df = prices_df.append(new_row, ignore_index=True)

# sau khi thêm thì những data vừa thêm đang xếp ở cuối danh sách, nên cần sort lại(tí nữa là loop cái dataframe này :3)
prices_df_sort_result = sort_prices_df(prices_df)

# khởi tạo mảng rỗng để sau khi cho hết data vào mảng này thì sẽ convert cái mảng này thành dataframe và in ra màn hình
calculate_result = []

# read sales.csv
sales_df = pd.read_csv(r'sales.csv')

# loop tất cả các phần tử của dataframe i bắt đầu từ 0
for i in range(len(prices_df_sort_result)):
    # lấy product id
    product_id = prices_df_sort_result.loc[i, "product_id"]

    # lấy ngày mà product đấy thay đổi giá
    updated_at = prices_df_sort_result.loc[i, "updated_at"]

    # lấy giá cũ của sản phẩm
    old_price = prices_df_sort_result.loc[i, "old_price"]

    # lấy giá mới của sản phẩm
    new_price = prices_df_sort_result.loc[i, "new_price"]

    # khởi tạo một biến sẽ chứa cái product id của row tiếp theo
    product_id_next = None

    # Tương tự :)
    updated_at_next = None

    # Chỗ này cần if vì nếu loop i chạy đến phần tử cuối cùng thì bị tràn index.
    # Kiểu có 10 row là mk cứ cố get dữ liệu của row 11 thì chương trình bị lỗi
    if i + 1 != len(prices_df_sort_result):
        # Lấy product id của row tiếp theo
        product_id_next = prices_df_sort_result.loc[i + 1, "product_id"]

        # Tượng tự :) :)
        updated_at_next = prices_df_sort_result.loc[i + 1, "updated_at"]

    # Nếu.. đoạn này gt dài quá bh t gt :v :v
    if product_id_next != product_id:
        sales = sales_df.loc[(sales_df['product_id'] == product_id) &
                             (sales_df['ordered_at'] >= updated_at)]
        revenue = (sales["quantity_ordered"].sum()) * new_price
        data_item = (product_id, old_price, new_price, updated_at, "null", revenue)
        calculate_result.append(data_item)
    else:
        sales = sales_df.loc[(sales_df['product_id'] == product_id) &
                             (sales_df['ordered_at'] >= updated_at) &
                             (sales_df['ordered_at'] < updated_at_next)]

        revenue = (sales["quantity_ordered"].sum()) * new_price
        data_item = (product_id, old_price, new_price, updated_at, updated_at_next, revenue)
        calculate_result.append(data_item)

# Add(covert) cái mảng mk vừa tính toán trong vòng for bên trên vào dataframe ( Tạo dataframe có các columns là ... ý)
final_result = pd.DataFrame(calculate_result, columns=['product_id', 'old_price', 'new_price','updated_at',
                                                       'updated_at_next','revenue'])

# In dataframe ra màn hình. Done <3 <3. K biết big data có lỗi k nhỉ :3
print (final_result)
