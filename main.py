# coding=utf-8
import pandas as pd

# read prices.csv
prices_df = pd.read_csv(r'prices.csv')
# read sales.csv
sales_df = pd.read_csv(r'sales.csv')

# sắp xếp prices dataframe prices_df theo product_id & updated_at
# c có thể searh "sort_values in pandas"
sort_result = prices_df.sort_values(by=['product_id', 'updated_at']).reset_index(drop=True)

# lấy row đầu tiên của mỗi group. Row đầu tiên là row mà product được update giá lần đầu.
# sau đó dựa vào các row này
# mk loop các phần tử trong này để thêm cái row 1/1/1900 0:00 vào df ban đầu (loop for ở dưới)
# c có thể searh "Pandas dataframe get first row of each group"
filter_result = sort_result.groupby('product_id').first().reset_index()

# for đi qua tất cả các phần tử của dataframe vừa filter i bắt đầu từ 0
for i in range(len(filter_result)):
    # định nghĩa 1 row mới có các colum là product_id, old_price, new_price, updated_at
    # row này là row mà mk có ý định thêm vào dataframe ban đầu(row mà 1/1/1900 0:00 đấy c).
    new_row = {'product_id': filter_result.loc[i, "product_id"],
               'old_price': 'null',  # h
               'new_price': filter_result.loc[i, "old_price"],
               'updated_at': '1/1/1900 0:00'  # h
               }
    # thêm row bên trên vào dataframe prices_df ban đầu.
    prices_df = prices_df.append(new_row, ignore_index=True)

# sau khi thêm thì những data vừa thêm đang xếp ở cuối danh sách, nên cần sort lại(tí nữa là loop cái dataframe này :3)
prices_df_sort_result = prices_df.sort_values(by=['product_id', 'updated_at']).reset_index(drop=True)

# khởi tạo mảng rỗng để sau khi cho hết data vào mảng này thì sẽ convert cái mảng này thành dataframe và in ra màn hình
calculate_result = []

# bắt đầu logic tính toán
# loop tất cả các phần tử của dataframe prices_df_sort_result i bắt đầu từ 0
for i in range(len(prices_df_sort_result)):
    # lấy product id của row i
    product_id = prices_df_sort_result.loc[i, "product_id"]

    # lấy ngày mà product đấy thay đổi giá của row i
    updated_at = prices_df_sort_result.loc[i, "updated_at"]

    # lấy giá cũ của sản phẩm của row i
    old_price = prices_df_sort_result.loc[i, "old_price"]

    # lấy giá mới của sản phẩmcủa row i
    new_price = prices_df_sort_result.loc[i, "new_price"]

    # khởi tạo một biến sẽ chứa cái product id của row tiếp theo(row i + 1)
    product_id_next = None

    # Tương tự :)
    updated_at_next = None

    # Chỗ này cần check if vì nếu loop i chạy đến phần tử cuối cùng thì bị tràn index.
    # Kiểu có 10 row mà c cứ cố get dữ liệu của row 11 thì chương trình bị lỗi
    if i + 1 != len(prices_df_sort_result):
        # Lấy product id của row tiếp theo i + 1
        product_id_next = prices_df_sort_result.loc[i + 1, "product_id"]

        # Tượng tự :) :)
        updated_at_next = prices_df_sort_result.loc[i + 1, "updated_at"]

    # Nếu product_id_next(id của row tiếp theo) mà k bằng id của row hiện tại
    # có nghĩa là là row này là row cuối cùng của nhóm r đấy
    if product_id_next != product_id:
        # lọc các row trong data frame sales_df có id >= updated_at
        # vì là row cuối cùng nên k có updated_at_next
        # k có lần update tiếp theo nữa đấy
        sales = sales_df.loc[(sales_df['product_id'] == product_id) &
                             (sales_df['ordered_at'] >= updated_at)]

        # tính toán doanh thu. Lấy tổng của quantity_ordered nhân với giá là được
        revenue = (sales["quantity_ordered"].sum()) * new_price

        # tạo ra item mới để add item này vào mảng calculate_result
        data_item = (product_id, old_price, new_price, updated_at, "null", revenue)

        # add item này vào mảng
        calculate_result.append(data_item)
    # còn không thì ngược lại id của row tiếp theo đang bằng id của row hiện tại
    # nghĩa là là chưa phải item cuối cùng của nhóm
    else:
        # lọc các row trong data frame sales_df có id >= updated_at và < updated_at_next
        # lọc sale trong khoảng time đấy c
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
