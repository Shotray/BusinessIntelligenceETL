from concurrent.futures import process
import csv

aminer_author = '../data/AMiner-Author/AMiner-Author.txt'
aminer_paper = '../data/AMiner-Paper/AMiner-Paper.txt'
aminer_coauthor = '../data/AMiner-Coauthor/AMiner-Coauthor.txt'

aminer_author_csv = '../data/AMiner-Author/AMiner-Author.csv'
aminer_author_interest_csv = '../data/AMiner-Author/AMiner-Author-Interest.csv'
aminer_author_affiliation_csv = '../data/AMiner-Author/AMiner-Author-Affiliation.csv'
aminer_paper_csv = '../data/AMiner-Paper/AMiner-Paper.csv'
# aminer_paper_author_affiliation_csv = '../data/AMiner-Paper/AMiner-Paper-Author-Affiliation.csv'
aminer_paper_affiliation_csv = '../data/AMiner-Paper/AMiner-Paper-Affiliation.csv'
aminer_paper_reference_csv = '../data/AMiner-Paper/Aminer-Paper-Reference.csv'
aminer_coauthor_csv = '../data/AMiner-Coauthor/AMiner-Coauthor.csv'
aminer_affiliation_csv = '../data/Aminer-Affiliation.csv'

def process_author(read_file,write_file,affiliation_write_file,interest_write_file):
    f_read = open(read_file,'r',encoding='utf-8')
    f_write = open(write_file,'w',encoding='utf-8',newline='')
    f_affiliation_write = open(affiliation_write_file,'w',encoding='utf-8',newline='')
    f_interest_write = open(interest_write_file,'w',encoding='utf-8',newline='')

    f_writer = csv.DictWriter(f_write,fieldnames=['index','name','pc','cn','hi','pi','upi'])
    f_writer.writeheader()

    f_affiliation_writer = csv.DictWriter(f_affiliation_write,fieldnames=['author_index','affiliation'])
    f_affiliation_writer.writeheader()
    f_interest_writer = csv.DictWriter(f_interest_write,fieldnames=['author_index','interest'])
    f_interest_writer.writeheader()

    author_dict = {'index':'','name':'','pc':'','cn':'','hi':'','pi':'','upi':''}
    affiliation_dict = {'author_index':'','affiliation':''}
    interest_dict = {'author_index':'','interest':''}

    per_author = []
    count = 0

    for line in f_read:
        line = line.replace("\"", " ").replace("\\", " ").replace("'", " ")
        count += 1
        if count % 100000 == 0:
                print(count)

        if line == '\n':
            continue
        per_author.append(line)     
        if line[:2]=='#t':
            
            author_dict['index'] = str(per_author[0])[7:-1]
            affiliation_dict['author_index'] = author_dict['index']
            interest_dict['author_index'] = author_dict['index']
            author_dict['name'] = str(per_author[1])[3:-1]
            # author_dict['affiliation'] =str(per_author[2][3:-1])
            author_dict['pc'] = str(per_author[3])[4:-1]
            author_dict['cn'] = str(per_author[4])[4:-1]
            author_dict['hi'] = str(per_author[5])[4:-1]
            author_dict['pi'] = str(per_author[6])[4:-1]
            author_dict['upi'] = str(per_author[7])[5:-1]
            # author_dict['t'] = str(per_author[8][3:-1])
            f_writer.writerow(author_dict)
            
            affiliation_list = str(per_author[2])[3:-1].split(';')
            if affiliation_list!=['']:
                for item in affiliation_list:
                    if item == '':
                        continue
                    affiliation_dict['affiliation'] = item.strip()
                    f_affiliation_writer.writerow(affiliation_dict)

            interest_list = str(per_author[8])[3:-1].split(';')
            if interest_list!=['']:
                for item in interest_list:
                    if item == '':
                        continue
                    interest_dict['interest'] = item.strip()
                    f_interest_writer.writerow(interest_dict)

            per_author = []
            affiliation_dict = affiliation_dict.fromkeys(['author_index','affiliation'],'')
            interest_dict = interest_dict.fromkeys(['author_index','interest'],'')

    f_read.close()
    f_write.close()
    f_affiliation_write.close()
    f_interest_write.close()

def process_paper(read_file,write_file,paper_affiliation_write_file):
    f_read = open(read_file,'r',encoding='utf-8')
    f_write = open(write_file,'w',encoding='utf-8',newline='')
    f_paper_affiliation_write = open(paper_affiliation_write_file,'w',encoding='utf-8',newline='')

    f_writer = csv.DictWriter(f_write,fieldnames=['index','paper_title','year','publication_venue','abstract'])
    f_writer.writeheader()

    f_paper_affiliation_writer = csv.DictWriter(f_paper_affiliation_write,fieldnames=['paper_index','affiliation'])
    f_paper_affiliation_writer.writeheader()

    paper_dict = {'index':'','paper_title':'','year':'','publication_venue':'','abstract':''}
    paper_affiliation_dict = {'paper_index':'','affiliation':''}

    count = 0

    for line in f_read:
        line = line.replace("\"", " ").replace("\\", " ").replace("'", " ")

        count += 1
        if count % 100000 == 0:
            print(count) 
        if line == '\n' or line[:2] == '#%':
            continue
        if line[:6]=='#index':
            if count > 1:
                f_writer.writerow(paper_dict)
            paper_dict = paper_dict.fromkeys(['index','paper_title','year','publication_venue','abstract'],'')
            paper_affiliation_dict = paper_affiliation_dict.fromkeys(['paper_index','affiliation'],'')
            paper_dict['index'] = line[7:-1]
            paper_affiliation_dict['paper_index'] = line[7:-1]
        elif line[:2] == '#*':
            paper_dict['paper_title'] = line[3:-1]
        # elif line[:2] == '#@':
        #     paper_affiliation_dict['author_name'] = line[3:-1]
        elif line[:2] == '#o':
            paper_affiliation_dict['affiliation'] = line[3:-1]
            affiliation_list = paper_affiliation_dict['affiliation'].split(';')
            if affiliation_list == ['']:
                continue
            temp_dict = {'paper_index':'','affiliation':''}
            last_dict = {'paper_index':'','affiliation':''}
            for i in range(len(affiliation_list)):
                if affiliation_list[i] == '-':
                    continue
                temp_dict['paper_index'] = paper_affiliation_dict['paper_index']
                temp_dict['affiliation'] = affiliation_list[i]
                if last_dict == temp_dict:
                    continue
                last_dict = temp_dict
                f_paper_affiliation_writer.writerow(temp_dict)
        elif line[:2] == '#t':
            paper_dict['year'] = line[3:-1]
        elif line[:2] == '#c':
            paper_dict['publication_venue'] = line[3:-1]
        elif line[:2] == '#!':
            paper_dict['abstract'] = line[3:-1]

    f_read.close()
    f_write.close()
    f_paper_affiliation_write.close()

def process_paper_references(read_file,write_file):
    f_read = open(read_file,'r',encoding='utf-8')
    f_write = open(write_file,'w',encoding='utf-8',newline='')

    f_writer = csv.DictWriter(f_write,fieldnames=['paper_index','referenced_index'])
    f_writer.writeheader()

    paper_dict = {'paper_index':'','referenced_index':''}

    count = 0

    for line in f_read:
        count += 1
        if count % 100000 == 0:
            print(count) 
        if line[:6]=='#index':
            paper_dict = paper_dict.fromkeys(['paper_index','referenced_index'],'')
            paper_dict['paper_index'] = line[7:-1]
        elif line[:2] == '#%':
            paper_dict['referenced_index'] = line[3:-1]
            f_writer.writerow(paper_dict)
        else:
            continue

    f_read.close()
    f_write.close()

def process_coauthor(read_file,write_file):
    f_read = open(read_file,'r',encoding='utf-8')
    f_write = open(write_file,'w',encoding='utf-8',newline='')

    f_writer = csv.DictWriter(f_write,fieldnames=['first_author','second_author','co_num'])
    f_writer.writeheader()

    coauthor_dict = {'first_author':'','second_author':'','co_num':''}
    
    for line in f_read:
        coauthor_list = line[1:].split()
        coauthor_dict['first_author'] = coauthor_list[0]
        coauthor_dict['second_author'] = coauthor_list[1]
        coauthor_dict['co_num'] = coauthor_list[2]
        f_writer.writerow(coauthor_dict)

def process_affiliation(read_file1,read_file2,write_file):
    f_read1 = open(read_file1,'r',encoding='utf-8')
    f_read2 = open(read_file2,'r',encoding='utf-8')
    f_write = open(write_file,'w',encoding='utf-8',newline='')

    reader1 = csv.DictReader(f_read1)
    reader2 = csv.DictReader(f_read2)

    f_writer = csv.DictWriter(f_write,fieldnames=['affiliation'])
    f_writer.writeheader()

    affiliation_dict = {'affiliation':''}

    affiliation_set = set()
    for line in reader1:
        affiliation_set.add(line['affiliation'])

    
    for line in reader2:
        affiliation_set.add(line['affiliation'])

    for index, item in enumerate(affiliation_set):
        if item == '':
            continue
        affiliation_dict['affiliation'] = item
        f_writer.writerow(affiliation_dict)
    
    f_read1.close()
    f_read2.close()
    f_write.close()
    
  

if __name__ == '__main__':
    # process_author(aminer_author,aminer_author_csv,aminer_author_affiliation_csv,aminer_author_interest_csv)
    # process_paper(aminer_paper,aminer_paper_csv,aminer_paper_affiliation_csv)
    # process_paper_references(aminer_paper,aminer_paper_reference_csv)
    # process_coauthor(aminer_coauthor,aminer_coauthor_csv)
    process_affiliation(aminer_author_affiliation_csv,aminer_paper_affiliation_csv,aminer_affiliation_csv)