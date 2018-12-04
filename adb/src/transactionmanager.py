from site import Sites


class TransactionManager:
    def __init__(self):
        self.sites_store_variable = dict()
        self.sites_map = dict()
        for i in range(1, 21):
            self.sites_store_variable[i] = []
            if i % 2 == 0:
                for k in range(1, 11):
                    self.sites_store_variable[i].append(k)
            else:
                self.sites_store_variable[i].append(i % 10 + 1)

        for i in range(1, 11):
            self.sites_map[i] = Sites(i)
