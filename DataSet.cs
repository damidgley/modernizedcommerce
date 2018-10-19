using System;
using System.IO;
//using System.Threading;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;
using System.Data.SqlClient;
using System.Windows.Forms;

namespace CTAP
{
    class DataSet
    {
        public static int SetArchiveRawFileSpec(String Currency, String GUID, DateTime Quote1, DateTime Quote2)
        {
            StreamReader FileReader = new StreamReader(Application.StartupPath + "\\ConnectionSetting.ini");
            String Connect = FileReader.ReadToEnd();
            FileReader.Close();

            SqlConnection ConnectDB = new SqlConnection();
            ConnectDB.ConnectionString = Connect;
            String QueryString = "INSERT INTO ArchiveRawFileSystem (Currency, GUID, FirstQuote, LastQuote, Normalized) VALUES ('" + Currency + "','" + GUID + "','" + Quote1.ToString() + "','" + Quote2.ToString() + "','0')";
            SqlCommand CommandDB = new SqlCommand(QueryString, ConnectDB);
            ConnectDB.Open();

            int CommandResult = CommandDB.ExecuteNonQuery();

            ConnectDB.Close();
            return CommandResult;
        }
        public static String[,] GetArchiveRawFileSpec(String Curr)
        {
            StreamReader FileReader = new StreamReader(Application.StartupPath + "\\ConnectionSetting.ini");
            String Connect = FileReader.ReadToEnd();
            FileReader.Close();

            SqlConnection ConnectDB = new SqlConnection();
            ConnectDB.ConnectionString = Connect;
            String QueryString = "SELECT GUID, FirstQuote, LastQuote FROM ArchiveRawFileSystem where (Currency = '" + Curr + "') AND (Normalized = 'False')";
            SqlCommand CommandDB = new SqlCommand(QueryString, ConnectDB);
            ConnectDB.Open();

            SqlDataReader reader = CommandDB.ExecuteReader();

            ArrayList TempResult = new ArrayList();
            while (reader.Read())
            {
                for (int i = 0; i <= 2; i++)
                    TempResult.Add(System.Convert.ToString(reader[i]));
            }

            reader.Close();
            ConnectDB.Close();

            if (TempResult.Count >= 3)
            {
                int Count = TempResult.Count / 3;
                String[,] SqlResult = new String[Count, 3];

                for (int i = 0; i < Count; i++)
                {
                    SqlResult[i, 0] = TempResult[i * 3].ToString();
                    SqlResult[i, 1] = TempResult[(i * 3) + 1].ToString();
                    SqlResult[i, 2] = TempResult[(i * 3) + 2].ToString();
                }

                return SqlResult;
            }
            else
            {
                return null;
            }
        }
        public static int SetArchiveRawFileNormalized(String GUID)
        {
            StreamReader FileReader = new StreamReader(Application.StartupPath + "\\ConnectionSetting.ini");
            String Connect = FileReader.ReadToEnd();
            FileReader.Close();

            SqlConnection ConnectDB = new SqlConnection();
            ConnectDB.ConnectionString = Connect;
            string QueryString = "UPDATE ArchiveRawFileSystem SET Normalized = '1' where GUID = '" + GUID + "'";
            SqlCommand CommandDB = new SqlCommand(QueryString, ConnectDB);
            ConnectDB.Open();

            int CommandResult = CommandDB.ExecuteNonQuery();

            ConnectDB.Close();
            return CommandResult;
        }
        public static DataSetFiles RequestDataSet(String Pair, String Owner, DateTime TimeStart, DateTime TimeEnd)
        {
            Boolean FileExists = false;
            Settings AppSet = AppSettings.GetSettings();
            Tic[] ArchFile;

            //Get the file spec for the archive file.
            DataSetFiles[] ArchFileSpec = GetDataSetFile(Pair, "Archival");

            //Get the file spec for existing dataset files.
            DataSetFiles[] DSFileSpec = GetDataSetFile(Pair, Owner);

            if (ArchFileSpec != null)
            {
                if (DSFileSpec == null)//No file exists.
                    DSFileSpec = new DataSetFiles[1];
                else
                {
                    if (DSFileSpec[0].FirstTic != TimeStart | DSFileSpec[0].LastTic != TimeEnd)
                        DataSet.DeleteDataSetFile(DSFileSpec[0].FileGUID);
                    else
                        FileExists = true;
                }
                if (FileExists == false)
                {
                    //Load the archive file.
                    ArchFile = LoadDataSet("\\ArchiveData\\",ArchFileSpec[0].FileGUID);

                    //Cut to size.  To begin with, work with ArchFile in terms of days rather than cutting it down to the minute.  
                    //Otherwise the IndexOfNormalized function cannot be used.
                    int StartIndex = DataSet.IndexOfNormalized(ref ArchFile, Convert.ToDateTime(TimeStart.ToString("d")));
                    int EndIndex = DataSet.IndexOfNormalized(ref ArchFile, Convert.ToDateTime(TimeEnd.ToString("d")).AddDays(1).AddMinutes(-1));
                    EndIndex = (EndIndex - StartIndex) + 1;
                    //Truncate the beginning
                    if (StartIndex > 0)
                        ArchFile = (Tic[])Utils.ResizeArrayTruncStart(ArchFile, StartIndex);
                    //Truncate the end
                    if (EndIndex > 0)
                        ArchFile = (Tic[])Utils.ResizeArray(ArchFile, EndIndex);


                    //Apply Exceptions list.

                    //Clean up.  For each minute that should be exluded from the file, mark the BID with 8686.  These minutes will
                    //not be saved to disk at the end of this routine.  Clean up beginning and trailing empty values.
                    Boolean Finito = false;
                    int Index = 0;
                    while (Index <= ArchFile.GetUpperBound(0) && Finito == false)
                    {
                        if (ArchFile[Index].Bid == 0)
                            ArchFile[Index].Bid = 8686;
                        else
                            Finito = true;
                        Index++;
                    }
                    Finito = false;
                    Index = ArchFile.GetUpperBound(0);
                    while (Index >= 0 && Finito == false)
                    {
                        if (ArchFile[Index].Bid == 0)
                            ArchFile[Index].Bid = 8686;
                        else
                            Finito = true;
                        Index--;
                    }

                    //Now narrow it down to the minute.
                    StartIndex = DataSet.IndexOfNormalized(ref ArchFile, Convert.ToDateTime(TimeStart.ToString("g")));
                    Index = 0;
                    if (StartIndex > 0)
                    {
                        while (Index < StartIndex)
                        {
                            ArchFile[Index].Bid = 8686;
                            Index++;
                        }
                    }
                    StartIndex = DataSet.IndexOfNormalized(ref ArchFile, Convert.ToDateTime(TimeEnd.ToString("g"))) + 1;
                    if (StartIndex > 0 && StartIndex <= ArchFile.GetUpperBound(0))
                    {
                        while (StartIndex <= ArchFile.GetUpperBound(0))
                        {
                            ArchFile[StartIndex].Bid = 8686;
                            StartIndex++;
                        }
                    }

                    //Fill
                    Finito = false;
                    Index = 0;
                    while (Index <= ArchFile.GetUpperBound(0) && Finito == false)
                    {
                        if (ArchFile[Index].Bid != 8686)
                            Finito = true;
                        else
                            Index++;
                    }
                    for (int x = Index; x <= ArchFile.GetUpperBound(0); x++)
                    {
                        if (ArchFile[x].Bid == 0)
                        {
                            ArchFile[x].Bid = ArchFile[x - 1].Bid;
                            ArchFile[x].Offer = ArchFile[x - 1].Offer;
                        }
                    }

                    //Mark all off-hour minutes as 86'd
                    ArrayList DaysContained = new ArrayList();
                    for (int x = 0; x <= ArchFile.GetUpperBound(0); x = x + 1440)
                        DaysContained.Add(ArchFile[x].Time.ToString("d"));
                    for (int x = 0; x < DaysContained.Count; x++)
                    {
                        DateTime TempDT = Convert.ToDateTime(DaysContained[x]);
                        if (TempDT.DayOfWeek == DayOfWeek.Saturday)
                        {
                            StartIndex = DataSet.IndexOfNormalized(ref ArchFile, Convert.ToDateTime(TempDT.ToString("d")));
                            EndIndex = DataSet.IndexOfNormalized(ref ArchFile, Convert.ToDateTime(TempDT.AddDays(1).AddMinutes(-1)));
                            DataSet.EightySix(ref ArchFile, StartIndex, EndIndex);
                        }
                        if (TempDT.DayOfWeek == DayOfWeek.Sunday)
                        {
                            StartIndex = DataSet.IndexOfNormalized(ref ArchFile, Convert.ToDateTime(TempDT.ToString("d")));
                            EndIndex = DataSet.IndexOfNormalized(ref ArchFile, TempDT.Date + Convert.ToDateTime(AppSet.EndofDay).TimeOfDay);
                            DataSet.EightySix(ref ArchFile, StartIndex, EndIndex);
                        }
                        if (TempDT.DayOfWeek == DayOfWeek.Friday)
                        {
                            StartIndex = DataSet.IndexOfNormalized(ref ArchFile, TempDT.Date + Convert.ToDateTime(AppSet.EndofDay).TimeOfDay);
                            EndIndex = DataSet.IndexOfNormalized(ref ArchFile, Convert.ToDateTime(TempDT.AddDays(1).AddMinutes(-1)));
                            DataSet.EightySix(ref ArchFile, StartIndex + 1, EndIndex);
                        }
                    }

                    //Save to File.
                    Guid Guido = Guid.NewGuid();
                    TextWriter FileWriter = new StreamWriter(AppSet.FileLocation + "\\DataSet\\" + Guido.ToString());
                    int Y = 0;
                    while (Y <= ArchFile.GetUpperBound(0))
                    {
                        if (ArchFile[Y].Bid != 8686)
                            FileWriter.WriteLine(ArchFile[Y].Time + "," + ArchFile[Y].Bid + "," + ArchFile[Y].Offer);
                        Y++;
                    }
                    FileWriter.Close();

                    //Update DB
                    DSFileSpec[0].Currency = Pair;
                    DSFileSpec[0].FileGUID = Guido.ToString();
                    DSFileSpec[0].FileUpdated = DateTime.Now;
                    DSFileSpec[0].Owner = Owner;
                    DSFileSpec[0].FirstTic = TimeStart;
                    DSFileSpec[0].LastTic = TimeEnd;
                    DataSet.SetDataSetFileSpec(DSFileSpec[0]);
                }
            }
           
            return DSFileSpec[0];
        }
        public static void EightySix(ref Tic[] TempLine, Int32 BeginIndex, Int32 EndIndex)
        {
            for (int x = BeginIndex; x <= EndIndex; x++)
                TempLine[x].Bid = 8686;
        }

        public static int IndexOfNormalized(ref Tic[] TempLine, DateTime SearchTime)
        {
            ArrayList DaysContained = new ArrayList();
            for (int x = 0; x <= TempLine.GetUpperBound(0); x = x + 1440)
                DaysContained.Add(TempLine[x].Time.ToString("d"));
            Boolean Found = false;
            int y = 0;
            while (y < DaysContained.Count && Found == false)
            {
                if (SearchTime.ToString("d") == Convert.ToDateTime(DaysContained[y]).ToString("d"))
                    Found = true;
                else
                    y++;
            }
            if (Found == true)//if found, return the index of the start of the search day plus the number of mins away from the beginning.
            {
                y = y * 1440;
                TimeSpan DateDiff = Convert.ToDateTime(SearchTime.ToString("g")) - Convert.ToDateTime(TempLine[y].Time.ToString("g"));
                y = y + (int)DateDiff.TotalMinutes;
            }
            else
                y = -1;
            return y;//if not found Index is -1.
        }

        public static int DeleteDataSetFile(String Guido)
        {
            Settings AppSet = AppSettings.GetSettings();
            String FileName = AppSet.FileLocation + "\\DataSet\\" + Guido;  
            FileInfo FI = new FileInfo(FileName);
            FI.Delete();

            StreamReader FileReader = new StreamReader(Application.StartupPath + "\\ConnectionSetting.ini");
            String Connect = FileReader.ReadToEnd();
            FileReader.Close();
            SqlConnection ConnectDB = new SqlConnection();
            ConnectDB.ConnectionString = Connect;
            String QueryString = "DELETE FROM DataSetFileSystem where GUID ='" + Guido + "'";
            SqlCommand CommandDB = new SqlCommand(QueryString, ConnectDB);
            ConnectDB.Open();

            int CommandResult = CommandDB.ExecuteNonQuery();

            ConnectDB.Close();
            return CommandResult;
        }
        public static Tic[] LoadDataSet(String Directory, String Guido)
        {
            String[] Separator = new String[] { "," };
            Settings AppSet = AppSettings.GetSettings();
            StreamReader FileReader = new StreamReader(AppSet.FileLocation + Directory + Guido);
            StringBuilder FileTemp = new StringBuilder();
            FileTemp.EnsureCapacity(10240);
            String TempSt = "";
            int Index = 0;
            while ((TempSt = FileReader.ReadLine()) != null)
            {
                FileTemp.Append("," + TempSt);
                Index++;
            }
            FileReader.Close();
            if (FileTemp.ToString().Length != 0)
                FileTemp.Remove(0, 1);
            String[] FileSplit = FileTemp.ToString().Split(Separator, StringSplitOptions.None);
            FileTemp = null;
            Tic[] ArchFile = new Tic[Index];
            for (int x = 0; x <= ArchFile.GetUpperBound(0); x++)
            {
                ArchFile[x].Time = Convert.ToDateTime(FileSplit[x * 3]);
                ArchFile[x].Bid = Convert.ToDecimal(FileSplit[(x * 3) + 1]);
                ArchFile[x].Offer = Convert.ToDecimal(FileSplit[(x * 3) + 2]);
            }
            return ArchFile;
        }
        public static DataSetFiles[] GetDataSetFile(String Pair, String Type)
        {
            StreamReader FileReader = new StreamReader(Application.StartupPath + "\\ConnectionSetting.ini");
            String Connect = FileReader.ReadToEnd();
            FileReader.Close();

            SqlConnection ConnectDB = new SqlConnection();
            ConnectDB.ConnectionString = Connect;
            String QueryString;
            if (Pair == "ALL")
                QueryString = "SELECT * FROM DataSetFileSystem where (Owner = '" + Type + "')";
            else
                QueryString = "SELECT * FROM DataSetFileSystem where (Currency = '" + Pair + "') AND (Owner = '" + Type + "')";
            SqlCommand CommandDB = new SqlCommand(QueryString, ConnectDB);
            ConnectDB.Open();

            SqlDataReader reader = CommandDB.ExecuteReader();

            int Count = 0;
            DataSetFiles FileSet = new DataSetFiles();
            DataSetFiles[] FileSets = new DataSetFiles[1];

            while (reader.Read())
            {
                FileSet.Currency = System.Convert.ToString(reader[0]);
                FileSet.Owner = System.Convert.ToString(reader[1]);
                FileSet.FirstTic = System.Convert.ToDateTime(reader[2]);
                FileSet.LastTic = System.Convert.ToDateTime(reader[3]);
                FileSet.FileUpdated = System.Convert.ToDateTime(reader[4]);
                FileSet.FileGUID = System.Convert.ToString(reader[5]);
                Count++;
                if (Count > FileSets.GetUpperBound(0) + 1)
                    FileSets = (DataSetFiles[])Utils.ResizeArray(FileSets, FileSets.Length + 1);

                FileSets[Count - 1] = FileSet;
            }

            reader.Close();
            ConnectDB.Close();

            if (Count > 0)
                return FileSets;
            else
                return null;
        }
        public static int SetDataSetFileSpec(DataSetFiles DSFile)
        {
            StreamReader FileReader = new StreamReader(Application.StartupPath + "\\ConnectionSetting.ini");
            String Connect = FileReader.ReadToEnd();
            FileReader.Close();

            SqlConnection ConnectDB = new SqlConnection();
            ConnectDB.ConnectionString = Connect;
            String QueryString = "INSERT INTO DataSetFileSystem (Currency, Owner, FirstTic, LastTic, FileUpdated, GUID) VALUES ('" + DSFile.Currency
                + "','" + DSFile.Owner + "','" + DSFile.FirstTic + "','" + DSFile.LastTic + "','" + DSFile.FileUpdated + "','" + DSFile.FileGUID + "')";
            SqlCommand CommandDB = new SqlCommand(QueryString, ConnectDB);
            ConnectDB.Open();

            int CommandResult = CommandDB.ExecuteNonQuery();

            ConnectDB.Close();
            return CommandResult;
        }
        public static int UpdateDataSetFileSpec(DataSetFiles DSFile)
        {
            StreamReader FileReader = new StreamReader(Application.StartupPath + "\\ConnectionSetting.ini");
            String Connect = FileReader.ReadToEnd();
            FileReader.Close();

            SqlConnection ConnectDB = new SqlConnection();
            ConnectDB.ConnectionString = Connect;
            String QueryString = "UPDATE DataSetFileSystem SET FirstTic = '" + DSFile.FirstTic + "', LastTic = '" + DSFile.LastTic + "', FileUpdated = '" + DSFile.FileUpdated + 
                "' where (Currency = '" + DSFile.Currency + "') AND (Owner = '" + DSFile.Owner + "')";
            SqlCommand CommandDB = new SqlCommand(QueryString, ConnectDB);
            ConnectDB.Open();

            int CommandResult = CommandDB.ExecuteNonQuery();

            ConnectDB.Close();
            return CommandResult;
        }
        public static int ClearExceptions()
        {
            StreamReader FileReader = new StreamReader(Application.StartupPath + "\\ConnectionSetting.ini");
            String Connect = FileReader.ReadToEnd();
            FileReader.Close();

            SqlConnection ConnectDB = new SqlConnection();
            ConnectDB.ConnectionString = Connect;
            String QueryString = "DELETE FROM Exceptions";
            SqlCommand CommandDB = new SqlCommand(QueryString, ConnectDB);
            ConnectDB.Open();

            int CommandResult = CommandDB.ExecuteNonQuery();

            ConnectDB.Close();
            return CommandResult;
        }
        public static Boolean SetExceptions(Exception[] Exceptions)
        {
            StreamReader FileReader = new StreamReader(Application.StartupPath + "\\ConnectionSetting.ini");
            String Connect = FileReader.ReadToEnd();
            FileReader.Close();

            SqlConnection ConnectDB = new SqlConnection();
            ConnectDB.ConnectionString = Connect;
            ConnectDB.Open();
            Boolean Result = true;
            for (int n = 0; n <= Exceptions.GetUpperBound(0); n++)
            {
                String QueryString = "INSERT INTO Exceptions (Pair, FromTime, ToTime, Description, Action) VALUES ('" + Exceptions[n].Pair
                    + "','" + Exceptions[n].From + "','" + Exceptions[n].To + "','" + Exceptions[n].Desc + "','" + Exceptions[n].Action + "')";
                SqlCommand CommandDB = new SqlCommand(QueryString, ConnectDB);
                int CommandResult = CommandDB.ExecuteNonQuery();
                if (CommandResult != 1)
                    Result = false;
            }
            ConnectDB.Close();
            return Result;
        }
        public static Exception[] GetExceptions()
        {
            StreamReader FileReader = new StreamReader(Application.StartupPath + "\\ConnectionSetting.ini");
            String Connect = FileReader.ReadToEnd();
            FileReader.Close();

            SqlConnection ConnectDB = new SqlConnection();
            ConnectDB.ConnectionString = Connect;
            String QueryString = "SELECT * FROM Exceptions";
            SqlCommand CommandDB = new SqlCommand(QueryString, ConnectDB);
            ConnectDB.Open();

            SqlDataReader reader = CommandDB.ExecuteReader();

            int Count = 0;
            Exception Except = new Exception();
            Exception[] Excepts = new Exception[1];

            while (reader.Read())
            {
                Except.Pair = System.Convert.ToString(reader[0]);
                Except.From = System.Convert.ToDateTime(reader[1]);
                Except.To = System.Convert.ToDateTime(reader[2]);
                Except.Desc = System.Convert.ToString(reader[3]);
                Except.Action = System.Convert.ToString(reader[4]);
                Count++;
                if (Count > Excepts.GetUpperBound(0) + 1)
                    Excepts = (Exception[])Utils.ResizeArray(Excepts, Excepts.Length + 1);

                Excepts[Count - 1] = Except;
            }

            reader.Close();
            ConnectDB.Close();

            if (Count > 0)
                return Excepts;
            else
                return null;
        }
        public static void TimeLineInit(ref Tic[] TimeLine, DateTime ADate)
        {
            TimeLine = (Tic[])Utils.ResizeArray(TimeLine, TimeLine.Length + 1440);
            for (int x = (TimeLine.GetUpperBound(0) + 1) - 1440; x <= TimeLine.GetUpperBound(0); x++)
            {
                TimeLine[x].Time = ADate;
                ADate = ADate.AddMinutes(1);
            }
        }

        public static Boolean IsOffHours()
        {
            Settings AppSet = AppSettings.GetSettings();

            Boolean IsBefore = true;
            Boolean IsAfter = true;
            DateTime ADate = Convert.ToDateTime("01/01/2001 " + Convert.ToDateTime(AppSet.EndofDay).TimeOfDay.ToString());
            DateTime NowDate = Convert.ToDateTime("01/01/2001 " + DateTime.Now.TimeOfDay.ToString());
            int DoArchive = NowDate.CompareTo(ADate);
            if (DoArchive > 0)
                IsBefore = false;
            ADate = Convert.ToDateTime("01/01/2001 " + Convert.ToDateTime(AppSet.EndofDay).AddMinutes(AppSet.ArchiveInterval).TimeOfDay.ToString());
            NowDate = Convert.ToDateTime("01/01/2001 " + DateTime.Now.TimeOfDay.ToString());
            DoArchive = NowDate.CompareTo(ADate);
            if (DoArchive < 0)
                IsAfter = false;

            //Check to see if current time falls during the weekend.  This is from end of day Friday to end of day Sunday.
            Boolean IsaGo = true;
            if (DateTime.Now.DayOfWeek == DayOfWeek.Friday & IsAfter == true)
                IsaGo = false;
            if (DateTime.Now.DayOfWeek == DayOfWeek.Saturday)
                IsaGo = false;
            if (DateTime.Now.DayOfWeek == DayOfWeek.Sunday & IsBefore == true)
                IsaGo = false;
            return IsaGo;
        }

        public static void Archive(String Pair, Boolean ReportProgress, BackgroundWorker Worker)
        {
            Currencies DSCurr = new Currencies();
            DSCurr.GetCurrencies(Pair);
            Settings AppSet = AppSettings.GetSettings();
            String[] Separator = new String[] { "," };

            //Make certain archiving does not happen during the weekend.  Make an exception for the end of the week.
            //When evaluating if the time is after the end of day on Friday, add the archive time interval to the end of day.
            //This will ensure that there will be at least one archive after the end of day, thus ensuring that we will get
            //all the data right up until the end of day on Friday. 
            Boolean IsBefore = true;
            Boolean IsAfter = true;
            DateTime ADate = Convert.ToDateTime("01/01/2001 " + Convert.ToDateTime(AppSet.EndofDay).TimeOfDay.ToString());
            DateTime NowDate = Convert.ToDateTime("01/01/2001 " + DateTime.Now.TimeOfDay.ToString());
            int DoArchive = NowDate.CompareTo(ADate);
            if (DoArchive > 0)
                IsBefore = false;
            ADate = Convert.ToDateTime("01/01/2001 " + Convert.ToDateTime(AppSet.EndofDay).AddMinutes(AppSet.ArchiveInterval).TimeOfDay.ToString());
            NowDate = Convert.ToDateTime("01/01/2001 " + DateTime.Now.TimeOfDay.ToString());
            DoArchive = NowDate.CompareTo(ADate);
            if (DoArchive < 0)
                IsAfter = false;

            //Check to see if current time falls during the weekend.  This is from end of day Friday to end of day Sunday.
            Boolean IsaGo = true;
            //if (DateTime.Now.DayOfWeek == DayOfWeek.Friday & IsAfter == true)
            //    IsaGo = false;
            //if (DateTime.Now.DayOfWeek == DayOfWeek.Saturday)
            //    IsaGo = false;
            //if (DateTime.Now.DayOfWeek == DayOfWeek.Sunday & IsBefore == true)
            //    IsaGo = false;
            if (IsaGo == false)
                return;
            for (int x = 0; x < DSCurr.Count; x++)
            {
                if (DSCurr.Curr[x].Archive == true)
                {
                    DataSetFiles[] DSFile = DataSet.GetDataSetFile(DSCurr.Curr[x].Name, "Archival");
                    if (DSFile == null)
                        DSFile = new DataSetFiles[1];

                    //Check the Last Tic.  If older than 24 hours, then specify 24 hours.
                    int TooOld = 0;
                    DateTime DayPast = DateTime.Now.Subtract(TimeSpan.FromHours(22));
                    if (DSFile[0].LastTic != Convert.ToDateTime("01/01/0001 12:00:00 AM"))
                        TooOld = DateTime.Compare(DSFile[0].LastTic, DayPast);
                    String StartDateTime;
                    DateTime PastTime;
                    if (DSFile[0].LastTic == Convert.ToDateTime("01/01/0001 12:00:00 AM") | TooOld < 0)
                        StartDateTime = DayPast.ToString("yyyy-MM-dd HH:mm:ss");
                    else
                    {
                        PastTime = Convert.ToDateTime(DSFile[0].LastTic.ToString());
                        PastTime = PastTime.AddHours(2);
                        PastTime = PastTime.AddSeconds(1);
                        StartDateTime = PastTime.ToString("yyyy-MM-dd HH:mm:ss");
                    }
                    DateTime FutureTime = DateTime.Now.AddHours(2);
                    String EndDateTime = FutureTime.ToString("yyyy-MM-dd HH:mm:ss");

                    //Now take into account that the StartTime may bleed into the weekend.
                    if (Convert.ToDateTime(StartDateTime).DayOfWeek == DayOfWeek.Sunday)
                    {
                        if (Convert.ToDateTime(StartDateTime).TimeOfDay < Convert.ToDateTime(AppSet.EndofDay).AddHours(2).TimeOfDay)
                        {
                            StartDateTime = Convert.ToDateTime(StartDateTime).ToString("d") + " " + Convert.ToDateTime(AppSet.EndofDay).TimeOfDay;
                            StartDateTime = Convert.ToDateTime(StartDateTime).AddHours(2).ToString("yyyy-MM-dd HH:mm:ss");
                        }
                    }

                    //Now call an HTTP function with Key, Pair and Start and End times.
                    String[] Param = new String[4];
                    Param[0] = "F0BB1355A30AC485C8E223178B6E5845";
                    Param[1] = DSCurr.Curr[x].Name;
                    Param[2] = StartDateTime;
                    Param[3] = EndDateTime;

                    DateTime LastUpdate = DateTime.Now;
                    String[] History = new String[2];
                    History = Http.HTTPGetHistory(Param, ReportProgress, Worker);

                    if (History[1] != "0")
                    {
                        //Save the data to file.
                        Guid Guido = Guid.NewGuid();
                        String TempFileName = Guido.ToString();
                        if (Directory.Exists(AppSet.FileLocation + "\\RawArchiveData\\") == false)
                            Directory.CreateDirectory(AppSet.FileLocation + "\\RawArchiveData\\");
                        TextWriter tw = new StreamWriter(AppSet.FileLocation + "\\RawArchiveData\\" + TempFileName);
                        tw.WriteLine(History[0]);
                        tw.Close();

                        //Get the First and Last Quote to update the form for saving archive file details.
                        //Apparently the data stream can get out of order.  So to determine the first and last quote
                        //I have to iterate throguht the whole thing.
                        String[] TempArray = History[0].Split(Separator, StringSplitOptions.None);
                        DateTime CompareDT1 = Convert.ToDateTime(TempArray[0]);
                        int Index = 3;
                        DateTime CompareDT2;
                        while (Index <= TempArray.GetUpperBound(0))
                        {
                            CompareDT2 = Convert.ToDateTime(TempArray[Index]);
                            if (CompareDT1.CompareTo(CompareDT2) > 0)
                                CompareDT1 = CompareDT2;
                            Index = Index + 3;
                        }
                        String FirstQuote = CompareDT1.ToString();
                        CompareDT1 = Convert.ToDateTime(TempArray[0]);
                        Index = 3;
                        while (Index <= TempArray.GetUpperBound(0))
                        {
                            CompareDT2 = Convert.ToDateTime(TempArray[Index]);
                            if (CompareDT1.CompareTo(CompareDT2) < 0)
                                CompareDT1 = CompareDT2;
                            Index = Index + 3;
                        }

                        //Update the DB and FileSet
                        DSFile[0].LastTic = CompareDT1;
                        DSFile[0].FileUpdated = LastUpdate;
                        int SetFileSpecResult = DataSet.SetArchiveRawFileSpec(DSCurr.Curr[x].Name, Guido.ToString(), Convert.ToDateTime(FirstQuote), CompareDT1);
                        if (SetFileSpecResult != 1)
                        {
                            MessageBox.Show("Error saving RawFileSpec to database.", "SQL Command Failure", MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
                            return;
                        }
                        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                        //Normalization
                        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                        //Determine if an archival file already exists for this currency.
                        Tic[] TimeLine = new Tic[0];
                        Boolean FileExists = false;
                        if (DSFile[0].FileGUID != null)
                            FileExists = true;

                        //Get a list of RAW files that contain quotes for the currency in question.  
                        String[,] Files = DataSet.GetArchiveRawFileSpec(DSCurr.Curr[x].Name);
                        if (Files != null)
                        {
                            //Build list of days involved.
                            ArrayList RAWDays = new ArrayList();
                            for (int i = 0; i <= Files.GetUpperBound(0); i++)
                            {
                                if (RAWDays.Count > 0)
                                {
                                    if (RAWDays.Contains(Convert.ToDateTime(Files[i, 1]).ToString("d")) == false)
                                        RAWDays.Add(Convert.ToDateTime(Files[i, 1]).ToString("d"));
                                }
                                else
                                    RAWDays.Add(Convert.ToDateTime(Files[i, 1]).ToString("d"));
                                if (RAWDays.Contains(Convert.ToDateTime(Files[i, 2]).ToString("d")) == false)
                                    RAWDays.Add(Convert.ToDateTime(Files[i, 2]).ToString("d"));
                            }
                            //The list has to be sorted because the method used to resize the timeline.
                            //There is probably an easier way to sort it.  All I know is that the array.sort is completely useless.
                            //First comvert the days to numbers
                            Index = 0;
                            while (Index < RAWDays.Count)
                            {
                                RAWDays[Index] = Convert.ToDateTime(RAWDays[Index]).ToOADate();
                                Index++;
                            }
                            //Sort the numbers.
                            RAWDays.Sort();
                            //Covert back to datetime format.
                            Index = 0;
                            while (Index < RAWDays.Count)
                            {
                                RAWDays[Index] = DateTime.FromOADate(Convert.ToDouble(RAWDays[Index]));
                                Index++;
                            }

                            //Create or update the archival file.  Keep track if the file existed for updating 
                            //the DB later.
                            //Archive files are based on the minute scale (1440 minutes per day).  Establish how many days have 
                            //to be added to an existing file or included in a new file.
                            if (FileExists == true)
                            {
                                //Read in the existing file.
                                String TempSt = "";
                                StringBuilder ArchiveFile = new StringBuilder();
                                ArchiveFile.EnsureCapacity(10240);
                                TextReader FileReader = new StreamReader(AppSet.FileLocation + "\\ArchiveData\\" + DSFile[0].FileGUID);
                                Index = 0;
                                while ((TempSt = FileReader.ReadLine()) != null)
                                {
                                    ArchiveFile.Append("," + TempSt);
                                    Index++;
                                }
                                ArchiveFile.Remove(0, 1);
                                FileReader.Close();
                                TimeLine = (Tic[])Utils.ResizeArray(TimeLine, Index);
                                String[] ArchiveFileSplit;
                                ArchiveFileSplit = ArchiveFile.ToString().Split(Separator, StringSplitOptions.None);
                                ArchiveFile = null;
                                for (int n = 0; n <= TimeLine.GetUpperBound(0); n++)
                                {
                                    TimeLine[n].Time = Convert.ToDateTime(ArchiveFileSplit[n * 3]);
                                    TimeLine[n].Bid = Convert.ToDecimal(ArchiveFileSplit[(n * 3) + 1]);
                                    TimeLine[n].Offer = Convert.ToDecimal(ArchiveFileSplit[(n * 3) + 2]);
                                }

                                //Build list of days included in the archive file.  Use this list of days to compare against the
                                //RAW list of days.  Only resize the timeline array for RAW days not included in the archival file.
                                ArrayList NormDays = new ArrayList();
                                for (int n = 0; n <= TimeLine.GetUpperBound(0); n = n + 1440)
                                    NormDays.Add(TimeLine[n].Time.ToString("d"));

                                //Resize and initialize the timeline.
                                //If the RAW day is in the list of Norm days, ignore it.
                                for (int i = 0; i < RAWDays.Count; i++)
                                {
                                    if (NormDays.Contains(Convert.ToDateTime(RAWDays[i]).ToString("d")) == false)
                                        DataSet.TimeLineInit(ref TimeLine, Convert.ToDateTime(RAWDays[i]));
                                }
                                NormDays = null;
                            }
                            else
                            {
                                //Resize and initialize the timeline.
                                for (int i = 0; i < RAWDays.Count; i++)
                                    DataSet.TimeLineInit(ref TimeLine, Convert.ToDateTime(RAWDays[i]));
                            }

                            //Establish the beginning index of the start of each day.  This will be used to help determine the index of
                            //each minute.  This is in lieu of searching the entire array which is too time consumming.
                            Int32[] DayStartIndex = new Int32[RAWDays.Count];
                            for (int n = 0; n < RAWDays.Count; n++)
                            {
                                Boolean Found = false;
                                int y = 0;
                                while (y <= TimeLine.GetUpperBound(0) && Found == false)
                                {
                                    if (TimeLine[y].Time.ToString("g") == Convert.ToDateTime(RAWDays[n]).ToString("g"))
                                        Found = true;
                                    else
                                        y = y + 1440;
                                }
                                if (Found == true)
                                    DayStartIndex[n] = y;
                            }

                            //Now process each file that contains quotes for each day.
                            for (int i = 0; i < RAWDays.Count; i++)
                            {
                                for (int n = 0; n <= Files.GetUpperBound(0); n++)
                                {
                                    if (Convert.ToDateTime(Files[n, 1]).ToString("d") == Convert.ToDateTime(RAWDays[i]).ToString("d") |
                                        Convert.ToDateTime(Files[n, 2]).ToString("d") == Convert.ToDateTime(RAWDays[i]).ToString("d"))
                                    {
                                        TextReader FileReader = new StreamReader(AppSet.FileLocation + "\\RawArchiveData\\" + Files[n, 0]);
                                        String RawFile = FileReader.ReadLine();
                                        FileReader.Close();

                                        String[] RawFileSplit = RawFile.Split(Separator, StringSplitOptions.None);
                                        RawFile = null;

                                        DateTime CurrentDay = Convert.ToDateTime(RAWDays[i]);
                                        Index = 0;
                                        while (Index <= RawFileSplit.GetUpperBound(0))
                                        {
                                            DateTime TempDate = Convert.ToDateTime(RawFileSplit[Index]);
                                            if (TempDate.ToString("d") == CurrentDay.ToString("d"))
                                            {
                                                int y = DayStartIndex[i];
                                                TimeSpan DateDiff = TempDate - Convert.ToDateTime(RAWDays[i]);
                                                y = y + (Int32)Math.Truncate(DateDiff.TotalMinutes);
                                                if (TimeLine[y].Bid == 0 | TimeLine[y].Time.CompareTo(TempDate) <= 0)
                                                {
                                                    TimeLine[y].Time = TempDate;
                                                    TimeLine[y].Bid = Convert.ToDecimal(RawFileSplit[Index + 1]);
                                                    TimeLine[y].Offer = Convert.ToDecimal(RawFileSplit[Index + 2]);
                                                }
                                            }
                                            Index = Index + 3;
                                        }
                                        RawFileSplit = null;

                                        //Mark the file as "Normalized" in the DB if the LastQuote fell within the current day.  
                                        if (Convert.ToDateTime(Files[n, 2]).ToString("d") == CurrentDay.ToString("d"))
                                        {
                                            int Result = DataSet.SetArchiveRawFileNormalized(Files[n, 0]);
                                            if (Result != 1)
                                            {
                                                MessageBox.Show("Error setting RAW file to Normalized.  GUID ID: " + Files[n, 0], "Database Error.", MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
                                                return;
                                            }
                                        }
                                    }
                                }
                            }

                            //Get the first tic of the normalized file in General.  We already have the last tic.
                            int Y = 0;
                            Boolean Finito = false;
                            while (Y <= TimeLine.GetUpperBound(0) && Finito == false)
                            {
                                if (TimeLine[Y].Bid != 0)
                                    Finito = true;
                                Y++;
                            }
                            if (Finito == true)
                                DSFile[0].FirstTic = TimeLine[Y - 1].Time;

                            //Save the file spec to DB and timeline to file.
                            if (FileExists == true)
                            {
                                TempFileName = DSFile[0].FileGUID;
                                DataSet.UpdateDataSetFileSpec(DSFile[0]);
                            }
                            else
                            {
                                Guido = Guid.NewGuid();
                                TempFileName = Guido.ToString();
                                DSFile[0].FileGUID = Guido.ToString();
                                DataSet.SetDataSetFileSpec(DSFile[0]);
                            }
                            if (Directory.Exists(AppSet.FileLocation + "\\ArchiveData\\") == false)
                                Directory.CreateDirectory(AppSet.FileLocation + "\\ArchiveData\\");
                            TextWriter FileWriter = new StreamWriter(AppSet.FileLocation + "\\ArchiveData\\" + TempFileName);
                            Y = 0;
                            while (Y <= TimeLine.GetUpperBound(0))
                            {
                                FileWriter.WriteLine(TimeLine[Y].Time + "," + TimeLine[Y].Bid + "," + TimeLine[Y].Offer);
                                Y++;
                            }
                            FileWriter.Close();
                        }
                    }
                    else
                    {
                        //MessageBox.Show("No Data Retrieved for " + Param[1] + ". From " + Param[2] + " To " + Param[3], "Data Error.", MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
                    }
                }
            }
            DSCurr = null;
        }
    }

    struct DataSetFiles
    {
        private String _Currency;
        private String _Owner;
        private DateTime _FirstTic;
        private DateTime _LastTic;
        private DateTime _FileUpdated;
        private String _FileGUID;

        public String Currency
        {
            get
            {
                return _Currency;
            }
            set
            {
                _Currency = value;
            }
        }
        public String Owner
        {
            get
            {
                return _Owner;
            }
            set
            {
                _Owner = value;
            }
        }
        public DateTime FirstTic
        {
            get
            {
                return _FirstTic;
            }
            set
            {
                _FirstTic = value;
            }
        }
        public DateTime LastTic
        {
            get
            {
                return _LastTic;
            }
            set
            {
                _LastTic = value;
            }
        }
        public DateTime FileUpdated
        {
            get
            {
                return _FileUpdated;
            }
            set
            {
                _FileUpdated = value;
            }
        }
        public String FileGUID
        {
            get
            {
                return _FileGUID;
            }
            set
            {
                _FileGUID = value;
            }
        }
        public DataSetFiles(String Currency, String Owner, DateTime FirstTic, DateTime LastTic, DateTime FileUpdated, String FileGUID)
        {
            _Currency = Currency;
            _Owner = Owner;
            _FirstTic = FirstTic;
            _LastTic = LastTic;
            _FileUpdated = FileUpdated;
            _FileGUID = FileGUID;
        }
    }
    public struct Tic
    {
        private DateTime _Time;
        private Decimal _Bid;
        private Decimal _Offer;
        private Decimal _STD;
        private Decimal _STD2;
        private Decimal _STD3;

        public DateTime Time
        {
            get
            {
                return _Time;
            }
            set
            {
                _Time = value;
            }
        }
        public Decimal Bid
        {
            get
            {
                return _Bid;
            }
            set
            {
                _Bid = value;
            }
        }
        public Decimal Offer
        {
            get
            {
                return _Offer;
            }
            set
            {
                _Offer = value;
            }
        }
        public Decimal STD
        {
            get
            {
                return _STD;
            }
            set
            {
                _STD = value;
            }
        }
        public Decimal STD2
        {
            get
            {
                return _STD2;
            }
            set
            {
                _STD2 = value;
            }
        }
        public Decimal STD3
        {
            get
            {
                return _STD3;
            }
            set
            {
                _STD3 = value;
            }
        }
        public Tic(DateTime Time, Decimal Bid, Decimal Offer, Decimal STD, Decimal STD2, Decimal STD3)
        {
            _Time = Time;
            _Bid = Bid;
            _Offer = Offer;
            _STD = STD;
            _STD2 = STD2;
            _STD3 = STD3;
        }
    }

    public struct Exception
    {
        private String _Pair;
        private DateTime _From;
        private DateTime _To;
        private String _Desc;
        private String _Action;

        public String Pair
        {
            get
            {
                return _Pair;
            }
            set
            {
                _Pair = value;
            }
        }
        public DateTime From
        {
            get
            {
                return _From;
            }
            set
            {
                _From = value;
            }
        }
        public DateTime To
        {
            get
            {
                return _To;
            }
            set
            {
                _To = value;
            }
        }
        public String Desc
        {
            get
            {
                return _Desc;
            }
            set
            {
                _Desc = value;
            }
        }
        public String Action
        {
            get
            {
                return _Action;
            }
            set
            {
                _Action = value;
            }
        }
        public Exception(String Pair, DateTime From, DateTime To, String Desc, String Action)
        {
            _Pair = Pair;
            _From = From;
            _To = To;
            _Desc = Desc;
            _Action = Action;
        }
    }
}