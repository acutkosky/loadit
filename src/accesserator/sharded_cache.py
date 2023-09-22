


    def get_key(self, path):
        return path.relative_to(self.dir)
        return int(path.stem.split(".")[0])

    def get_path_for_key(self, key):
        return path.dir / key
        eligible_paths = sorted(
            self.dir.glob(f"{key}.*.shard.pickle"),
            key=lambda x: int(x.stem.split(".")[1]),
        )
        if len(eligible_paths) == 0:
            raise FileNotFoundError

        return eligible_paths[-1]

