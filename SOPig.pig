REGISTER sopig.jar;

DEFINE FIRST_ANSWER_AWAIT_TIME com.aj.sopig.FIRST_ANSWER_AWAIT_TIME();
DEFINE ROW_COUNT(X) RETURNS Z { Y = group $X all; $Z = foreach Y generate COUNT($X); };

--"posts input: /data/posts.xml'
--"users input: /data/users.xml'

DBPosts = LOAD '$posts' using com.aj.sopig.SoCustomPostsLoader() AS (
  id:chararray,
  parentId:chararray,
  postTypeId:chararray,
  acceptedAnswerId:chararray,
  creationDate:chararray,
  score:long,
  viewCount:long,
  body:chararray,
  ownerUserId:chararray,
  lastEditorUserId:chararray,
  lastEditorDisplayName:chararray,
  lastEditDate:chararray,
  lastActivityDate:chararray,
  title:chararray,
  tags:bag{},
  answerCount:int,
  commentCount:int,
  favoriteCount:int);

DBUsers = LOAD '$users' using com.aj.sopig.SoCustomUsersLoader() AS (
  id:chararray,
  reputation:long,
  creationDate:chararray,
  displayName:chararray,
  emailHash:chararray,
  lastAccessedDate:chararray,
  views:long,
  upVotes:long,
  downVotes:long);

--"Filter the data into 2 sets (questions and answers)"
Questions = FILTER DBPosts BY postTypeId == '1';
Answers = FILTER DBPosts BY postTypeId == '2';

NumberOfQuestions = ROW_COUNT(Questions);
STORE NumberOfQuestions INTO '$resultsfolder/NumberOfQuestionsTotal' USING PigStorage(',');

NumberOfAnswers = ROW_COUNT(Answers);
STORE NumberOfAnswers INTO '$resultsfolder/NumberOfAnswersTotal' USING PigStorage(',');

--"Find Questions/Answers/Users per month"
QuestionsPerMonthYearGrp = GROUP Questions BY (year, month);
QuestionsPerMonthYear = FOREACH QuestionsPerMonthYearGrp GENERATE group as tt, COUNT(Questions) as q;
STORE QuestionsPerMonthYear INTO '$resultsfolder/QuestionsPerMonthYear' USING PigStorage(',');

AnswersPerMonthYearGrp = GROUP Answers BY (year, month);
AnswersPerMonthYear = FOREACH AnswersPerMonthYearGrp GENERATE group as tt, COUNT(Answers) as a;
STORE AnswersPerMonthYear INTO '$resultsfolder/AnswersPerMonthYear' USING PigStorage(',');

UsersPerMonthYearGrp = GROUP DBUsers BY (year, month);
UsersPerMonthYear = FOREACH UsersPerMonthYearGrp GENERATE group as tt, COUNT(DBUsers) as u;
STORE UsersPerMonthYear INTO '$resultsfolder/UsersPerMonthYear' USING PigStorage(',');

--"Find most/least scored questions"
MostScoredQuestionsAll = ORDER Questions BY score DESC;
MostScoredQuestions100 = LIMIT MostScoredQuestionsAll 100;
STORE MostScoredQuestions100 INTO '$resultsfolder/MostScoredQuestions100' USING PigStorage(',');

LeastScoredQuestionsAll = ORDER Questions BY score ASC;
LeastScoredQuestions10 = LIMIT LeastScoredQuestionsAll 10;
STORE LeastScoredQuestions10 INTO '$resultsfolder/LeastScoredQuestions10' USING PigStorage(',');

--"Find most/least scored answers"
MostScoredAnswersAll = ORDER Answers BY score DESC;
MostScoredAnswers100 = LIMIT MostScoredAnswersAll 100;
STORE MostScoredAnswers100 INTO '$resultsfolder/MostScoredAnswers100' USING PigStorage(',');

LeastScoredAnswersAll = ORDER Answers BY score ASC;
LeastScoredAnswers10 = LIMIT LeastScoredAnswersAll 10;
STORE LeastScoredAnswers10 INTO '$resultsfolder/LeastScoredAnswers10' USING PigStorage(',');

--"Find Average anserCount"
QGrp = GROUP Questions ALL;
AvgNumberOfAnswers = FOREACH QGrp GENERATE AVG(Questions.answerCount);
STORE AvgNumberOfAnswers INTO '$resultsfolder/AverageNumberOfAnswersPerQuestion' USING PigStorage(',');

--"Find Average questionCount"
AvgNumberOfComments = FOREACH QGrp GENERATE AVG(Questions.commentCount);
STORE AvgNumberOfComments INTO '$resultsfolder/AverageNumberOfCommentsPerQuestion' USING PigStorage(',');

--"Find Average viewCount"
AvgViewCount = FOREACH QGrp GENERATE AVG(Questions.viewCount);
STORE AvgViewCount INTO '$resultsfolder/AverageViewCount' USING PigStorage(',');

--"Find Question Day Distribution"
QByDayGrp = GROUP Questions BY dayOfWeek;
NumberOfQuestionsPerDay = FOREACH QByDayGrp GENERATE group as tt, COUNT(Questions) as q;
STORE NumberOfQuestionsPerDay INTO '$resultsfolder/NumberOfQuestionsPerDayDistrib' USING PigStorage(',');

--"Find Answers Day Distribution"
AByDayGrp = GROUP Answers BY dayOfWeek;
NumberOfAnswersPerDay = FOREACH AByDayGrp GENERATE group as tt, COUNT(Answers) as a;
STORE NumberOfAnswersPerDay INTO '$resultsfolder/NumberOfAnswersPerDayDistrib' USING PigStorage(',');

--"Find Questions Per Hour distribution"
QByHourGrp = GROUP Questions BY hour;
NumberOfQuestionsPerHour = FOREACH QByHourGrp GENERATE group as tt, COUNT(Questions) as q;
STORE NumberOfQuestionsPerHour INTO '$resultsfolder/NumberOfQuestionsPerHourDistrib' USING PigStorage(',');

--"Find Answers Per Hour distribution"
AByHourGrp = GROUP Answers BY hour;
NumberOfAnswersPerHour = FOREACH AByHourGrp GENERATE group as tt, COUNT(Answers) as a;
STORE NumberOfAnswersPerHour INTO '$resultsfolder/NumberOfAnswersPerHourDistrib' USING PigStorage(',');

--"Filter early"
QuestionsWithAnswers = FILTER Questions BY answerCount > 0;

--"Answered Questions Count"
NumberOfQuestionsWithAnswers = ROW_COUNT(QuestionsWithAnswers);
STORE NumberOfQuestionsWithAnswers INTO '$resultsfolder/NumberOfQuestionsWithAnswers' USING PigStorage(',');

--"Connect questions with answers"
QuestionsWithRelatedAnswers = COGROUP QuestionsWithAnswers BY id, Answers BY parentId;

--"Clean up the structure a bit to get rid of the rest of the fields"
AnsFilt = FOREACH QuestionsWithRelatedAnswers GENERATE FIRST_ANSWER_AWAIT_TIME($0,$1.creationDate,$2.creationDate) AS TimeTuple;
AnsCleared = FOREACH AnsFilt GENERATE TimeTuple.$0 as qid:int, TimeTuple.$1 as atime:long;
AnsClearedAndFilt = FILTER AnsCleared BY atime > 0;

--"Calculate avg response time"
JustTime = FOREACH AnsClearedAndFilt GENERATE $1 as ati;
JustTimeGrp = GROUP JustTime ALL;
AverageResponseTime = FOREACH JustTimeGrp GENERATE AVG(JustTime);
STORE AverageResponseTime INTO '$resultsfolder/AvgFirstAnswerTime' USING PigStorage(',');

--"Number of Questions Per tag"
Tags = FOREACH Questions GENERATE id, tags, year, month;
TagsFlat = FOREACH Tags GENERATE id as id, year as year, month as month, FLATTEN(tags) as tag;
TagsGrp = GROUP TagsFlat BY tag;
NumberOfQuestionsPerTag = FOREACH TagsGrp GENERATE group as tt, COUNT(TagsFlat) as cc;
STORE NumberOfQuestionsPerTag INTO '$resultsfolder/NumberOfQuestionsPerTag' USING PigStorage(',');

--"Calculate Number of Questions Per Tag Per Year/Month"
TagsGrpByMonth = GROUP TagsFlat BY  (tag, year, month);
NumberOfQuestionsPerTagPerMonth = FOREACH TagsGrpByMonth GENERATE group as tt, COUNT(TagsFlat) as cc;
STORE NumberOfQuestionsPerTagPerMonth INTO '$resultsfolder/NumberOfQuestionsPerTagPerMonth' USING PigStorage(',');

--"Calculate Number of Questions Per Month"
QPerMonthGrp = GROUP Questions BY (year, month);
NumberOfQuestionsPerMonth = FOREACH QPerMonthGrp GENERATE group as tt, COUNT(Questions) as q;
STORE NumberOfQuestionsPerMonth INTO '$resultsfolder/NumberOfQuestionsPerMonth' USING PigStorage(',');

--"Number of Favorite Questions
FavoriteQuestions = FILTER Questions BY favoriteCount > 0;
NumberOfFavoriteQuestions = ROW_COUNT(FavoriteQuestions);
STORE NumberOfFavoriteQuestions INTO '$resultsfolder/NumberOfFavQuestions' USING PigStorage(',');

--"Top most favorite questions"
MostFavoriteQuestionsAll = ORDER FavoriteQuestions BY favoriteCount DESC;
MostFavoriteQuestions100 = LIMIT MostFavoriteQuestionsAll 100;
STORE MostFavoriteQuestions100 INTO '$resultsfolder/MostFavoriteQuestions100' USING PigStorage(',');

--"TOP Users by upVotes/downVotes/Reputation/Views"
TopUsersUpVotesAll = ORDER DBUsers BY upVotes DESC;
TopUsersUpVotes100 = LIMIT TopUsersUpVotesAll 100;
STORE TopUsersUpVotes100 INTO '$resultsfolder/TopUsersUpVotes100' USING PigStorage(',');

TopUsersReputationAll = ORDER DBUsers BY reputation DESC;
TopUsersReputation100 = LIMIT TopUsersReputationAll 100;
STORE TopUsersReputation100 INTO '$resultsfolder/TopUsersReputation100' USING PigStorage(',');