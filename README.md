# Expert Finding in Legal Community Question Answering
Arian Askari, Suzan Verberne, and Gabriella Pasi. *Expert Finding in Legal Community Question Answering*. ECIR 2022 (short).

If you use this work, please cite as:

```
@InProceedings{Askari2022ECIR,
  author = {Askari, Arian and Verberne, Suzan and Pasi, Gabriella},
  title = {Expert Finding in Legal Community Question Answering},
  booktitle = {ECIR},
  year = {2022}
}
```
## Dataset

### Queries
All queries are are available in `/data/queries_bankruptcy.csv`

### Posts
The link of all posts that have been used for this reaserch (in Bankruptcy category) are available in `/data/question_links_bankruptcy.json`

### Labels
The labels are provided in qrel format (queyr_id itteration user_id label) in `/data/labels.qrel`. Itteration is always zero and not used. Query id refers to id of query in queries_bankruptcy.csv file, user id refers to the lawyer id, label is zero for non-expert and one for expert users.

### Lawyers webpage on Avvo
The lawyers' webpage addresses are available in `lawyerid_to_lawyerurl.json` in `{"user id": "lawyer url"}` format. Therefore, the lawyer ids (user ids) in labels.csv can be mapped to their webpage on Avvo by this file.

P.S: All pages were stored anonymously during this research with regard to the users' privacy.
