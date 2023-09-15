# The Ordering Dimension
# - blocks vs transactions are the same dimension, just different levels of granularity
# - selections in the ordering dimension are
#     - sometimes over a single point in time
#     - sometimes over a range of time


def freeze(query, datasets):
    # a query starts off as a seires of lists of chunks
    subqueries = partition_query(query, partition_by)
    for dataset in datasets:
        for subquery in subqueries:
            collect_partition(subquery, dataset)


# break a query into subqueries
def partition_query(query, partition_by):
    return [
        create_parition_query(partition, query)
        for partition in create_partitions(query, partition_by):
    ]


def collect_partition(query, dataset):
    for request in get_query_requests(query):
        dataset.perform_request(request)
    df = results_to_df()


