import pandas as pd
from pandas import DataFrame,Series
import numpy as np

#input
aone_data = "input/daiken.csv"
url_category = "input/daiken_category_list.csv"

prism3_master = 'master/prism3_middle_category.csv'
prism1_master = 'master/prism1_master.csv'


class TFDataMakerClass:

    def __init__(self,aone_data,prism1_master,prism3_master,category_master):
        
        self.aone_data = aone_data
        self.prism3_master = prism3_master
        self.category_master = category_master
        self.prism1_master = prism1_master
        
        #初期ファイルの読み込み
        self.df = pd.read_csv(aone_data)
        # tuuid がnullなレコードを削除
        self.df = self.df[~self.df['tuuid'].str.contains('null')].reset_index(drop=True)
        
        
        #データ処理
        #prism3,category
        self.prism3_process()
        print('prism3 process end')

        self.url_category_process()
        print('url category process end')
        self.union_data()
        print('union end')

        self.total_catdf.to_csv(path_or_buf="./output/prism3_cat_out.csv",index=False)
        print('prism3_cat file output')
        
        #prism1
        self.prism1_process()
        print('prism1 end')

        #ファイル出力
        self.p1df.to_csv(path_or_buf="./output/prism1_out.csv",index=False)
        print('prism1 file output')

        
      
    
    def prism3_process(self):
                #prism3 マスタファイル読み込み
        self.prism3_master_df = pd.read_csv(self.prism3_master,header=None)
        self.prism3_master_df.columns = ['prism3','category_name']
        print('prism3 read end')
        
        #prism3 dataframe 抽出
        self.prism3_df = self.df.loc[:,["tuuid","prism3_category_ids"]]
        self.prism3_df = self.prism3_df.where(self.prism3_df.prism3_category_ids != 'null').dropna().drop_duplicates('tuuid',keep='last').reset_index(drop=True)
        
        print('prism3 df generate end')
        
        
        #横→縦
        self.prism3v_df = DataFrame(columns=['tuuid','prism3'])
        self.prism3_ver_trans()
        print('prism3 ver trans end')
        
        #merge
        self.prism3_merge()
        print('prism3 merge end')

 
    def prism3_ver_trans(self):
        #リスト化されたPrism3テーブルを立て持ちに変換する
        print(len(self.prism3_df.index))
        lines = []
        for key,row in self.prism3_df.iterrows():
            pids = row[1].split('*')
            for pid in pids:
                line = [row[0],pid]
                lines.append(line)
                
        self.prism3v_df = DataFrame(lines)
        self.prism3v_df.columns = ['tuuid','prism3']
        self.prism3v_df.prism3 = self.prism3v_df.prism3.astype('int')


    
    def prism3_merge(self):
        #縦持ちのprism3テーブルに日本語を付与
        self.p3_merge_df = pd.merge(self.prism3v_df,self.prism3_master_df,how='left')
        #欠損値削除
        self.p3_merge_df = self.p3_merge_df.loc[self.p3_merge_df.prism3 > 0]
        
    
    
                
                
    def url_category_process(self):
        #url_category マスタファイル読み込み
        self.category_master_df = pd.read_csv(self.category_master)
        self.category_master_df = self.category_master_df.loc[:,["カテゴリID","カテゴリ名1","URL"]]
        self.category_master_df.columns = ["cat_id",'category_name','URL']
        
        #url_category　抽出
        self.url_category_df = self.df.loc[:,["tuuid",'url_category_ids']]
        #null削除
        self.url_category_df = self.url_category_df.where(self.url_category_df.url_category_ids != 'null').dropna()
       

        #横→縦
        self.url_categoryv_df = DataFrame(columns=['tuuid','cat_id'])
        self.url_category_ver_trans()
        
        print("url_tran end")
        #merge
        self.url_category_merge()
        
        

    def url_category_ver_trans(self):

        #リスト化されたPrism3テーブルを立て持ちに変換する
        print(len(self.url_category_df.index))
        lines = []
        for key,row in self.url_category_df.iterrows():
            pids = row[1].split('*')
           
            for pid in pids:
                line = [row[0],pid]
                lines.append(line)
            if(key % 100000 == 0):
                print(key)
        
        self.url_categoryv_df = DataFrame(lines)
        self.url_categoryv_df.columns = ["tuuid","cat_id"]
        self.url_categoryv_df.cat_id = self.url_categoryv_df.cat_id.astype('int')
            
    def url_category_merge(self):
        #縦持ちのprism3テーブルに日本語を付与
        self.cat_merge_df = pd.merge(self.url_categoryv_df,self.category_master_df,how='left')
        #欠損値削除
        self.cat_merge_df = self.cat_merge_df.loc[self.cat_merge_df.cat_id > 0]
        # self.cat_merge_df.to_csv(path_or_buf="cat_out.csv",index=False)
                  
    
    def union_data(self):
        tmp_prism3 = self.p3_merge_df.drop("prism3",1)
        tmp_cat = self.cat_merge_df.drop(['cat_id','URL'],1)
        self.total_catdf = pd.concat([tmp_prism3, tmp_cat], ignore_index=True).drop_duplicates().sort_values(by='tuuid').dropna().reset_index(drop=True)

    
    #todo: prism1 table last作成
    def prism1_process(self):
        #prism1 マスタファイルの読み込み
        self.prism1_master_df = pd.read_csv(self.prism1_master)
        self.prism1_master_df.columns = ['prism1','category_name']
        self.prism1_mearge()
        
        
        
        
    
    def prism1_mearge(self):
        self.prism1_df = self.df.loc[:,['time','tuuid','gender_age','occupation','income']]
        self.prism1_df.time = self.prism1_df.time.astype('str')
        #nullを0で置換
        self.prism1_df = self.prism1_df.where(self.prism1_df != 'null',0)
        
        #型をint64へ変換
        self.prism1_df = self.prism1_df.astype({'gender_age':int,'occupation':int,'income':int})
        
        #merge
        p1df = pd.merge(self.prism1_df,self.prism1_master_df,left_on='gender_age',right_on='prism1',how='left')
        p1df = p1df.drop(['gender_age','prism1'],1).rename(columns={"category_name" : "gender_age"})
        
        p1df = pd.merge(p1df,self.prism1_master_df,left_on='occupation',right_on='prism1',how='left')
        p1df = p1df.drop(['occupation','prism1'],1).rename(columns={"category_name" : "occupation"})
        
        p1df = pd.merge(p1df,self.prism1_master_df,left_on='income',right_on='prism1',how='left')
        p1df = p1df.drop(['income','prism1'],1).rename(columns={"category_name" : "income"})
        p1df.sort_values('time').reset_index(drop=True)
        
        #重複、欠損値削除
        self.p1df = p1df.drop_duplicates(['tuuid', 'gender_age','occupation','income'],keep='last').dropna(thresh=2).drop("time",1)
        
        
                
        
TM = TFDataMakerClass(aone_data,prism1_master,prism3_master,url_category)


        
    
