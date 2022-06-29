# coding=utf-8
import pandas as pd

pd.options.display.width = None
pd.options.display.max_columns = None
pd.set_option('display.max_rows', 3000)
pd.set_option('display.max_columns', 3000)

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

# sau khi thêm thì những data vừa thêm đang xếp ở cuối danh sách, nên cần sort lại.
prices_df_sort_result = prices_df.sort_values(by=['product_id', 'updated_at']).reset_index(drop=True)

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX thảo luận bắt đầu từ đây
# phải convert ordered_at thành kiểu datetime thì mới merge_asof được. Các line dưới lý do tương tụ :)
sales_df.ordered_at = pd.to_datetime(sales_df.ordered_at)

# phải convert updated_at thành kiểu datetime
prices_df_sort_result.updated_at = pd.to_datetime(prices_df_sort_result.updated_at)

# phải order theo ordered_at
sales_df_orderby = sales_df.sort_values('ordered_at')

# phải order theo updated_at
prices_df_sort_result_order_by = prices_df_sort_result.sort_values('updated_at')

# join 2 bảng theo product id và lấy giá trị gần nhất của ordered_at và updated_at
join_result = pd.merge_asof(sales_df_orderby, prices_df_sort_result_order_by, by='product_id',
                            left_on=['ordered_at'], right_on=['updated_at'])

# group xong thì kq sẽ thành 1 cái dataframe mới có 3 column product_id, new_price, quantity_ordered
group_result = join_result.groupby(["product_id", "new_price"])["quantity_ordered"].count().reset_index()

# tạo ra column mới là tổng của quantity_ordered và new_price
revenue = group_result["new_price"] * group_result["quantity_ordered"]
group_result["revenue"] = revenue

# in ra màn hình và suy đoán !!!
print (group_result)

