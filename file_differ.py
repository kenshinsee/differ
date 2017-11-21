import pandas as pd, numpy as np
import os

class File_Differ:
    def __init__(self, meta):
        self.meta = meta
        self.sort_column = meta["sort_column"]
        self.tolerance = meta["tolerance"]
        self.file_a = meta["file_a"]["file_loc"]
        self.file_b = meta["file_b"]["file_loc"]
        for file in [self.file_a, self.file_b]:
            if not os.path.isfile(file):
                raise FileNotFoundError("%s doesn't exist." % file)
        
        # create dataframe based on files
        self.df_a = pd.read_csv(self.file_a, names=self.meta["file_a"]["headers"])
        self.df_b = pd.read_csv(self.file_b, names=self.meta["file_b"]["headers"])
        
        # create and merge dfs, add identical flag for each columns
        self.merged_df = self.__merge_dataframe(self.df_a, self.df_b, "outer", meta["file_a"]["keys"], meta["file_b"]["keys"], True)
        columns_in_df_a_b = self.__get_dataframe_column_names(meta["diff_fields"])
        self.columns_in_df_a = columns_in_df_a_b["file_a"]
        self.columns_in_df_b = columns_in_df_a_b["file_b"]
        self.__fill_na_inplace( self.merged_df, self.columns_in_df_a + self.columns_in_df_b )
        diff_columns = self.__add_identical_flag(self.merged_df, self.columns_in_df_a, self.columns_in_df_b)
        ## now we start reordering the columns in merged_df, put comparable columns together
        column_pairs = list(zip(self.columns_in_df_a, self.columns_in_df_b, diff_columns))
        column_pairs_to_list = ['is_identical', '_merge']
        for pair in column_pairs:
            column_pairs_to_list = column_pairs_to_list + list(pair)
        self.merged_df_reordered = self.merged_df[meta["file_a"]["keys"] + meta["file_b"]["keys"] + column_pairs_to_list]
        
        # aggregate
        agg_df_a = self.__create_agg_dataframe(self.df_a, "file_a")
        agg_df_b = self.__create_agg_dataframe(self.df_b, "file_b")
        self.merged_agg_df = self.__merge_dataframe(agg_df_a, 
            agg_df_b, 
            "inner", 
            meta["diff_agg_fields"]["file_a"]["group_by"], 
            meta["diff_agg_fields"]["file_b"]["group_by"], 
            True)
        raw_agg_a_columns = [ i[0] for i in meta["diff_agg_fields"]["file_a"]["aggregate"] ]
        raw_agg_b_columns = [ i[0] for i in meta["diff_agg_fields"]["file_b"]["aggregate"] ]
        agg_fields = list(zip(raw_agg_a_columns, raw_agg_b_columns))
        columns_in_agg_df_a_b = self.__get_dataframe_column_names(agg_fields)
        columns_in_agg_df_a = columns_in_agg_df_a_b["file_a"]
        columns_in_agg_df_b = columns_in_agg_df_a_b["file_b"]
        tolerance_columns = self.__add_diff_pctg_flag(self.merged_agg_df, columns_in_agg_df_a, columns_in_agg_df_b, self.tolerance)
        ## now we start reordering the columns in merged_agg_df, put comparable columns together
        column_pairs = list(zip(columns_in_agg_df_a, columns_in_agg_df_b, tolerance_columns))
        column_pairs_to_list = ['diff_in_tolerance']
        for pair in column_pairs:
            column_pairs_to_list = column_pairs_to_list + [pair[0]] + [pair[1]] + pair[2]
        self.merged_agg_df_reordered = self.merged_agg_df[meta["diff_agg_fields"]["file_a"]["group_by"] + meta["diff_agg_fields"]["file_b"]["group_by"] + column_pairs_to_list]
        
        # summary info
        self.a_count = self.df_a.shape[0]
        self.b_count = self.df_b.shape[0]
        self.exist_in_both_count = self.merged_df[self.merged_df["_merge"] == "both"].shape[0]
        self.a_only_count = self.merged_df[self.merged_df["_merge"] == "left_only"].shape[0]
        self.b_only_count = self.merged_df[self.merged_df["_merge"] == "right_only"].shape[0]
        self.columns_dont_match_count = self.merged_df[(self.merged_df["is_identical"] == 0) & (self.merged_df["_merge"] == "both")].shape[0]
        self.diff_not_in_tolerance = self.merged_agg_df[self.merged_agg_df["diff_in_tolerance"] == 0].shape[0]
        
        self.summary_value = [self.a_count, 
                              self.b_count, 
                              self.exist_in_both_count, 
                              self.a_only_count, 
                              self.b_only_count, 
                              self.columns_dont_match_count,
                              self.tolerance,
                              self.diff_not_in_tolerance,
                             ]
        self.summary_index = ["a_count [aFile]", 
                              "b_count [bFile]",
                              "exist_in_both_count [Detail]", 
                              "a_only_count [aOnly]", 
                              "b_only_count [bOnly]",
                              "columns_dont_match [notAllMatch]", 
                              "tolerance", 
                              "diff_not_in_tolerance [notInTolerance]"
                             ]
                
    def __merge_dataframe(self, df_a, df_b, how, left_on, right_on, indicator):
        return pd.merge(df_a, df_b, how=how, 
             left_on = left_on, 
             right_on = right_on, 
             indicator = indicator)
             
    def __get_dataframe_column_names(self, field_pairs):
    # If one column has same name in two dataframe, 
    # will name the first one with _x suffix and 
    # the second one with _y suffix as per pandas column naming convention
        merged_file_a_columns = []
        merged_file_b_columns = []
        for pair in field_pairs:
            if pair[0] == pair[1]:
                merged_file_a_columns.append(pair[0] + "_x")
                merged_file_b_columns.append(pair[1] + "_y")
            else:
                merged_file_a_columns.append(pair[0])
                merged_file_b_columns.append(pair[1])
        return {"file_a": merged_file_a_columns, "file_b": merged_file_b_columns}
    
    def __fill_na_inplace(self, df, cols):
        nan_dic = {}
        for i in cols:
            nan_dic.__setitem__(i, 'NAN')
        df.fillna(nan_dic, inplace=True)

    def __add_identical_flag(self, df, file_a_cols, file_b_cols):
        column_pairs = list(zip(file_a_cols, file_b_cols))
        df["is_identical"] = 1
        identical_flags = []
        for pair in column_pairs:
            identical_flags.append(pair[0] + "_identical")
            df[pair[0] + "_identical"] = df[list(pair)].apply(lambda x: 1 if x[0] == x[1] else 0, axis=1)
            df["is_identical"] = df[[pair[0] + "_identical", "is_identical"]].apply(lambda x: 0 if x[0] == 0 or x[1] == 0 else 1, axis=1)
        return identical_flags
        
    def __create_agg_dataframe(self, df, file_ind):
        agg_func = {
            "avg": np.mean, 
            "sum": np.sum,
            "max": np.max,
            "min": np.min,
            "count": np.count_nonzero,
        }
        agg_call = {}
        groupby = self.meta["diff_agg_fields"][file_ind]['group_by']
        for agg in self.meta["diff_agg_fields"][file_ind]['aggregate']:
            agg_call.__setitem__(agg[0], agg_func[agg[1]])

        agg_columns = [ i[0] for i in self.meta["diff_agg_fields"][file_ind]['aggregate'] ]
        if len(agg_columns) != len(set(agg_columns)):
            raise ValueError("It's not allowed to have one column appleared more than once in diff_agg_fields->file_a/file_b->aggregate")
        return df[groupby + agg_columns].groupby(self.meta["diff_agg_fields"][file_ind]['group_by'], as_index=False).aggregate(agg_call)

    def __add_diff_pctg_flag(self, df, file_a_cols, file_b_cols, tolerance):
        column_pairs = list(zip(file_a_cols, file_b_cols))
        df["diff_in_tolerance"] = 1
        tolerance_flags = []
        for pair in column_pairs:
            tolerance_flags.append([pair[0] + "_diff", pair[0] + "_diff_in_tolerance"])
            df[pair[0] + "_diff"] = df[list(pair)].apply(lambda x: abs((x[0] - x[1]) / x[0]), axis=1)
            df[pair[0] + "_diff_in_tolerance"] = df[list(pair)].apply(lambda x: 1 if abs((x[0] - x[1]) / x[0]) <= tolerance else 0, axis=1)
            df["diff_in_tolerance"] = df[[pair[0] + "_diff_in_tolerance", "diff_in_tolerance"]].apply(lambda x: 0 if x[0] == 0 or x[1] == 0 else 1, axis=1)
        return tolerance_flags
        
    def get_df_a(self):
        return self.df_a
    
    def get_df_b(self):
        return self.df_b
    
    def get_dataframe(self):
        return self.merged_df
    
    def get_df_not_match(self):
        if self.sort_column: 
            return self.merged_df_reordered[(self.merged_df_reordered["is_identical"] == 0) & (self.merged_df_reordered["_merge"] == "both")]
        else:
            return self.merged_df[(self.merged_df["is_identical"] == 0) & (self.merged_df["_merge"] == "both")]
    
    def get_agg_dataframe(self):
        return self.merged_agg_df
    
    def get_agg_not_in_tolerance(self):
        if self.sort_column:
            return self.merged_agg_df_reordered[self.merged_agg_df["diff_in_tolerance"] == 0]
        else:
            return self.merged_agg_df[self.merged_agg_df["diff_in_tolerance"] == 0]
        
        
    def get_a_only(self):
        return self.merged_df[self.merged_df["_merge"] == 'left_only'][self.meta["file_a"]["keys"] + self.columns_in_df_a]
    
    def get_b_only(self):
        return self.merged_df[self.merged_df["_merge"] == 'right_only'][self.meta["file_b"]["keys"] + self.columns_in_df_b]
    
    def get_summary(self):
        self.summary_df = pd.DataFrame(data=self.summary_value, index=self.summary_index, columns=["summary"])
        return self.summary_df
