class Partitioner(object):
		def __init__(self, conn):
			self.conn = conn;
			#parameters
			# tolerance rate \mu
			# storage overhead \gamma
			# the current checkout cost: curCheckoutCost

		# invoked during the commit
		# if commonCnt < prev_delta * |R| and S < gamma, 
		# 	create a new version (partition?)
		# else 
		# 	add it to its parent partition table
		# update_partition_table()
		# compare_with_opt()
		# migration()
		def commit_assign():

		# update the partition table with the corresponding partition mapping
		def update_partition_table():

		#migration from one partition scheme to the opt one
		def migration():

		# check if current average checkout cost > \mu * optimal one (opt_partition())
		def exceeds_tolarated_checkout_cost():
			# return curCheckoutCost > \mu * opt_partition()
			return False

		def opt_partition():
			# return optimal checkout cost


		# pick the edge to cut
		def pick_edge():

		# return the corresponding partition table of the given vid
		def lookup():
