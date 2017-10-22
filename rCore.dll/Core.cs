using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.IO;
using rCore.Functions;
using rCore.Structures;

namespace rCore
{
    public class Core
    {
        #region Properties

        public Encoding Encoding = Encoding.Default;
        protected string structurePath;
        protected List<LuaField> fieldList;
        protected string rdbPath;
        protected string date = string.Empty;
        protected List<Row> data = new List<Row>();
        protected LUA luaIO;

        #endregion

        #region Constructors

        public Core() { }

        public Core(Encoding encoding) { this.Encoding = encoding; }

        #endregion

        #region Events

        public event EventHandler<ProgressMaxArgs> ProgressMaxChanged;
        public event EventHandler<ProgressValueArgs> ProgressValueChanged;
        public event EventHandler<MessageArgs> MessageOccured;

        #endregion

        #region Event Delegates

        public void OnProgressMaxChanged(ProgressMaxArgs p) { ProgressMaxChanged?.Invoke(this, p); }
        public void OnProgressValueChanged(ProgressValueArgs p) { ProgressValueChanged?.Invoke(this, p); }
        public void OnMessageOccured(MessageArgs m) { MessageOccured?.Invoke(this, m); }

        #endregion

        #region Get Methods

        public List<Row> Data { get { return data; } }

        public int FieldCount { get { return fieldList.Count; } }

        public List<LuaField> FieldList { get { return fieldList; } }

        public LuaField GetField(int idx) { return fieldList[idx]; }

        public LuaField GetField(string name) { return fieldList.Find(f => f.Name == name); }

        public int GetFieldIdx(string name) { return fieldList.FindIndex(f => f.Name == name); }

        public bool UseRowProcesser { get { return luaIO.UseRowProcessor; } }

        public bool SpecialCase { get { return luaIO.SpecialCase; } }

        public bool UseSelectStatement { get { return luaIO.UseSelectStatement; } }

        public string SelectStatement { get { return luaIO.SelectStatement; } }

        public bool UseSqlColumns { get { return luaIO.UseSqlColumns; } }

        public SqlCommand InsertStatement
        {
            get
            {
                SqlCommand sqlCmd = new SqlCommand();
                string columns = string.Empty;
                string parameters = string.Empty;

                if (UseSqlColumns)
                {
                    List<string> sqlColumns = luaIO.SqlColumns;
                    int colCount = sqlColumns.Count;

                    OnProgressMaxChanged(new ProgressMaxArgs(colCount));

                    for (int colIdx = 0; colIdx < colCount; colIdx++)
                    {
                        string val = sqlColumns[colIdx];
                        string columnType = GetField(val).Type;
                        columns += string.Format("{0}{1},", val, string.Empty);
                        parameters += string.Format("@{0}{1},", val, string.Empty);
                        SqlDbType paramType = SqlDbType.Int;

                        switch (columnType)
                        {
                            case "short":
                                paramType = SqlDbType.SmallInt;
                                break;

                            case "ushort":
                                paramType = SqlDbType.SmallInt;
                                break;

                            case "int":
                                paramType = SqlDbType.Int;
                                break;

                            case "uint":
                                paramType = SqlDbType.Int;
                                break;

                            case "long":
                                paramType = SqlDbType.BigInt;
                                break;

                            case "byte":
                                paramType = SqlDbType.TinyInt;
                                break;

                            case "datetime":
                                paramType = SqlDbType.DateTime;
                                break;

                            case "decimal":
                                paramType = SqlDbType.Decimal;
                                break;

                            case "single":
                                paramType = SqlDbType.Float;
                                break;

                            case "double":
                                paramType = SqlDbType.BigInt;
                                break;

                            case "string":
                                paramType = SqlDbType.NVarChar;
                                break;
                        }
                        sqlCmd.Parameters.Add(val, paramType);

                        if (((colIdx * 100) / colCount) != ((colIdx - 1) * 100 / colCount)) { OnProgressValueChanged(new ProgressValueArgs(colIdx)); }
                    }
                }
                else
                {
                    int colCount = fieldList.Count;

                    OnProgressMaxChanged(new ProgressMaxArgs(colCount));

                    for (int colIdx = 0; colIdx < colCount; colIdx++)
                    {
                        LuaField field = fieldList[colIdx];

                        columns += string.Format("{0}{1},", field.Name, string.Empty);
                        parameters += string.Format("@{0}{1},", field.Name, string.Empty);
                        SqlDbType paramType = SqlDbType.Int;

                        switch (field.Type)
                        {
                            case "short":
                                paramType = SqlDbType.SmallInt;
                                break;

                            case "ushort":
                                paramType = SqlDbType.SmallInt;
                                break;

                            case "int":
                                paramType = SqlDbType.Int;
                                break;

                            case "uint":
                                paramType = SqlDbType.Int;
                                break;

                            case "long":
                                paramType = SqlDbType.BigInt;
                                break;

                            case "byte":
                                paramType = SqlDbType.TinyInt;
                                break;

                            case "datetime":
                                paramType = SqlDbType.DateTime;
                                break;

                            case "decimal":
                                paramType = SqlDbType.Decimal;
                                break;

                            case "single":
                                paramType = SqlDbType.Float;
                                break;

                            case "double":
                                paramType = SqlDbType.BigInt;
                                break;

                            case "string":
                                paramType = SqlDbType.NVarChar;
                                break;
                        }
                        sqlCmd.Parameters.Add(field.Name, paramType);

                        if (((colIdx * 100) / colCount) != ((colIdx - 1) * 100 / colCount)) { OnProgressValueChanged(new ProgressValueArgs(colIdx)); }
                    }
                }

                OnProgressValueChanged(new ProgressValueArgs(0));
                OnProgressMaxChanged(new ProgressMaxArgs(100));

                sqlCmd.CommandText = string.Format("INSERT INTO <tableName> ({0}) VALUES ({1})", columns.Remove(columns.Length - 1, 1), parameters.Remove(parameters.Length - 1, 1));
                return sqlCmd;
            }
        }

        public string Case { get { return luaIO.Case; } }

        public Row GetRow(int idx) { return (Row)data[idx]; }

        public int RowCount { get { return data.Count; } }

        public string CreatedDate { get { return date; } }

        #endregion

        #region Methods (Public)

        internal void CallRowProcessor(string mode, Row row, int rowCount) { luaIO.CallRowProcessor(mode, row, rowCount); }

        public void SetEncoding(Encoding encoding) { this.Encoding = encoding; }

        public void SetData(List<Row> data) { this.data = data; }

        public void ClearData() { data.Clear(); }

        public void Initialize(string structurePath)
        {
            luaIO = new LUA(IO.LoadStructure(structurePath));
            fieldList = luaIO.GetFieldList();
        }

        public void ParseRDB(string rdbPath)
        {
            if (File.Exists(rdbPath))
            {
                using (MemoryStream ms = new MemoryStream(File.ReadAllBytes(rdbPath)))
                {
                    byte[] buffer = new byte[8];
                    ms.Read(buffer, 0, buffer.Length);

                    date = this.Encoding.GetString(buffer);

                    ms.Position += 120;

                    // Read the row count
                    buffer = new byte[4];
                    ms.Read(buffer, 0, buffer.Length);

                    if (SpecialCase)
                    {
                        switch (Case)
                        {
                            case "doubleloop":

                                int rows = BitConverter.ToInt32(buffer, 0);

                                for (int rowIdx = 0; rowIdx < rows; rowIdx++)
                                {
                                    buffer = new byte[4];
                                    ms.Read(buffer, 0, buffer.Length);

                                    int loopCount = BitConverter.ToInt32(buffer, 0);
                                    for (int i = 0; i < loopCount; i++)
                                    {
                                        Row currentRow = readRow(ms);
                                        if (UseRowProcesser) { CallRowProcessor("read", currentRow, rowIdx); }
                                        data.Add(currentRow);
                                        if (((rowIdx * 100) / RowCount) != ((rowIdx - 1) * 100 / RowCount)) { OnProgressValueChanged(new ProgressValueArgs(rowIdx)); }
                                    }
                                }

                                break;
                        }
                    }
                    else
                    {
                        int rows = BitConverter.ToInt32(buffer, 0);

                        OnProgressMaxChanged(new ProgressMaxArgs(rows));

                        for (int rowIdx = 0; rowIdx < rows; rowIdx++)
                        {
                            Row currentRow = readRow(ms);
                            if (UseRowProcesser) { CallRowProcessor("read", currentRow, rowIdx); }
                            data.Add(currentRow);

                            if (((rowIdx * 100) / rows) != ((rowIdx - 1) * 100 / rows)) { OnProgressValueChanged(new ProgressValueArgs(rowIdx)); }
                        }
                    }
                }

                OnProgressMaxChanged(new ProgressMaxArgs(100));
                OnProgressValueChanged(new ProgressValueArgs(0));
            }
            else { throw new FileNotFoundException("Cannot find file specified", rdbPath); }
        }

        public void WriteRDB(string buildPath)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                writeHeader(ms);

                OnProgressMaxChanged(new ProgressMaxArgs(RowCount));

                if (SpecialCase)
                {
                    byte[] buffer;

                    switch (Case)
                    {
                        case "doubleloop":

                            int previousVal = 0;
                            int loopCount = 0;
                            int subLoopCount = 0;

                            for (int rowIdx = 0; rowIdx < RowCount; rowIdx++)
                            {
                                int currentVal = (int)((Row)data[rowIdx]).ValueByFlag("loopcounter");
                                if (previousVal != currentVal) { previousVal = currentVal; loopCount++; }
                            }

                            buffer = BitConverter.GetBytes(loopCount);
                            ms.Write(buffer, 0, buffer.Length);

                            previousVal = 0;

                            for (int rowIdx = 0; rowIdx < RowCount; rowIdx++)
                            {
                                Row currentRow = (Row)data[rowIdx];
                                string currentValKey = currentRow.KeyByFlag("loopcounter");
                                int currentVal = (int)currentRow.ValueByFlag("loopcounter");
                                if (previousVal != currentVal)
                                {
                                    foreach (Row row in data) { if ((int)row[currentValKey] == currentVal) { subLoopCount++; } }
                                    previousVal = currentVal;

                                    buffer = BitConverter.GetBytes(subLoopCount);
                                    ms.Write(buffer, 0, buffer.Length);
                                }

                                if (UseRowProcesser) { CallRowProcessor("write", currentRow, rowIdx); }
                                writeRow(ms, currentRow);
                                if (((rowIdx * 100) / RowCount) != ((rowIdx - 1) * 100 / RowCount)) { OnProgressValueChanged(new ProgressValueArgs(rowIdx)); }
                            }

                            break;
                    }
                }
                else
                {
                    ms.Write(BitConverter.GetBytes(RowCount), 0, 4);

                    for (int rowIdx = 0; rowIdx < RowCount; rowIdx++)
                    {
                        Row currentRow = (Row)data[rowIdx];

                        if (UseRowProcesser) { CallRowProcessor("write", currentRow, rowIdx); }
                        writeRow(ms, currentRow);

                        if (((rowIdx * 100) / RowCount) != ((rowIdx - 1) * 100 / RowCount)) { OnProgressValueChanged(new ProgressValueArgs(rowIdx)); }
                    }
                }

                OnMessageOccured(new MessageArgs(string.Format("Writing {0}", buildPath)));
                using (FileStream fs = File.Create(buildPath)) { ms.WriteTo(fs); }
            }

            OnProgressMaxChanged(new ProgressMaxArgs(100));
            OnProgressValueChanged(new ProgressValueArgs(0));
        }

        #endregion

        #region Methods (Private)

        private BitVector32 generateBitVector(Row currentRow, string fieldName)
        {
            List<Cell> cells = currentRow.GetBitFromVectorFields(fieldName);
            BitVector32 bitVector = currentRow.GetBitVector(fieldName);

            foreach (Cell cell in cells) { bitVector[1 << cell.Info.Position] = Convert.ToBoolean(cell.Value); }
            return bitVector;
        }

        private byte[] readStream(MemoryStream ms, int size)
        {
            byte[] buffer = new byte[size];
            ms.Read(buffer, 0, buffer.Length);
            return buffer;
        }

        private Row readRow(MemoryStream ms)
        {
            Row currentRow = new Row(fieldList);

            for (int fieldIdx = 0; fieldIdx < currentRow.Count; fieldIdx++)
            {
                LuaField currentField = GetField(fieldIdx);

                switch (currentField.Type)
                {
                    case "short":
                        currentRow[fieldIdx] = BitConverter.ToInt16(readStream(ms, currentField.Length), 0);
                        break;

                    case "ushort":
                        currentRow[fieldIdx] = BitConverter.ToUInt16(readStream(ms, currentField.Length), 0);
                        break;

                    case "int":
                        currentRow[fieldIdx] = BitConverter.ToInt32(readStream(ms, currentField.Length), 0);
                        break;

                    case "uint":
                        currentRow[fieldIdx] = BitConverter.ToUInt32(readStream(ms, currentField.Length), 0);
                        break;

                    case "long":
                        currentRow[fieldIdx] = BitConverter.ToUInt64(readStream(ms, currentField.Length), 0);
                        break;

                    case "byte":
                        currentRow[fieldIdx] = (int)readStream(ms, currentField.Length)[0];
                        break;

                    case "bitvector":
                        currentRow[fieldIdx] = new BitVector32(BitConverter.ToInt32(readStream(ms, currentField.Length), 0));
                        break;

                    case "bitfromvector":
                        int bitPos = currentRow.GetPosition(fieldIdx);
                        BitVector32 bitVector = (BitVector32)currentRow.GetBitVector(currentField.BitsName);
                        currentRow[fieldIdx] = Convert.ToInt32(bitVector[1 << bitPos]);
                        break;

                    case "datetime":
                        int val = BitConverter.ToInt32(readStream(ms, currentField.Length), 0);
                        currentRow[fieldIdx] = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddSeconds(val);
                        break;

                    case "decimal":
                        currentRow[fieldIdx] = BitConverter.ToInt32(readStream(ms, currentField.Length), 0) / 100;
                        break;

                    case "single":
                        currentRow[fieldIdx] = BitConverter.ToSingle(readStream(ms, currentField.Length), 0);
                        break;

                    case "double":
                        currentRow[fieldIdx] = BitConverter.ToDouble(readStream(ms, currentField.Length), 0);
                        break;

                    case "string":
                        currentRow[fieldIdx] = ByteConverterExt.ToString(readStream(ms, currentField.Length), Encoding);
                        break;

                    case "stringlen":
                        currentRow[fieldIdx] = BitConverter.ToInt32(readStream(ms, currentField.Length), 0);
                        break;

                    case "stringbylen":
                        currentRow[fieldIdx] = ByteConverterExt.ToString(readStream(ms, currentRow.GetStringLen(currentField.Name)), Encoding);
                        break;
                }
            }

            return currentRow;
        }

        private void writeRow(MemoryStream ms, Row currentRow)
        {
            byte[] buffer;

            for (int fieldIdx = 0; fieldIdx < FieldCount; fieldIdx++)
            {
                LuaField currentField = GetField(fieldIdx);

                switch (currentField.Type)
                {
                    case "short":
                        ms.Write(BitConverter.GetBytes((short)currentRow[fieldIdx]), 0, currentField.Length);
                        break;

                    case "ushort":
                        ms.Write(BitConverter.GetBytes((ushort)currentRow[fieldIdx]), 0, currentField.Length);
                        break;

                    case "int":
                        ms.Write(BitConverter.GetBytes(Convert.ToInt32(currentRow[fieldIdx])), 0, currentField.Length);
                        break;

                    case "uint":
                        ms.Write(BitConverter.GetBytes((uint)currentRow[fieldIdx]), 0, currentField.Length);
                        break;

                    case "long":
                        ms.Write(BitConverter.GetBytes((long)currentRow[fieldIdx]), 0, currentField.Length);
                        break;

                    case "byte":
                        int fieldLen = currentRow.GetLength(fieldIdx);
                        if (fieldLen == 1) { ms.WriteByte(Convert.ToByte(currentRow[fieldIdx])); }
                        else { ms.Write(new byte[fieldLen], 0, fieldLen); }
                        break;

                    case "bitvector":
                        ms.Write(BitConverter.GetBytes(generateBitVector(currentRow, currentField.Name).Data), 0, currentField.Length);
                        break;

                    case "datetime":
                        DateTime val = Convert.ToDateTime(currentRow[currentField.Name]);
                        int val2 = Convert.ToInt32((val - new DateTime(1970, 1, 1, 0, 0, 0, 0)).TotalSeconds);
                        ms.Write(BitConverter.GetBytes(val2), 0, currentField.Length);
                        break;

                    case "decimal":
                        ms.Write(BitConverter.GetBytes(Convert.ToInt32(currentRow[fieldIdx]) * 100), 0, currentField.Length);
                        break;

                    case "single":
                        ms.Write(BitConverter.GetBytes((Single)currentRow[fieldIdx]), 0, currentField.Length);
                        break;

                    case "double":
                        ms.Write(BitConverter.GetBytes((double)currentRow[fieldIdx]), 0, currentField.Length);
                        break;

                    case "string":
                        buffer = ByteConverterExt.ToBytes(currentRow[fieldIdx].ToString(), Encoding);
                        int remainder = currentField.Length - buffer.Length;
                        ms.Write(buffer, 0, buffer.Length);
                        ms.Write(new byte[remainder], 0, remainder);
                        break;

                    case "stringlen":
                        ms.Write(BitConverter.GetBytes(currentRow.GetStringByLenValue(currentField.Name).Length + 1), 0, currentField.Length);
                        break;

                    case "stringbylen":
                        buffer = ByteConverterExt.ToBytes(currentRow[fieldIdx].ToString() + '\0', Encoding);
                        ms.Write(buffer, 0, buffer.Length);
                        break;
                }
            }
        }

        private void writeHeader(MemoryStream ms)
        {
            byte[] header = new byte[128];
            byte[] date = this.Encoding.GetBytes(string.Format("{0}{1}{2}", DateTime.Now.Year, GetDate(DateTime.Now.Month), GetDate(DateTime.Now.Day))); ;
            for (int i = 0; i < date.Length; i++) { header[i] = date[i]; }
            ms.Write(header, 0, header.Length);
        }

        private string GetDate(int pInt)
        {
            if (pInt >= 10)
                return pInt.ToString();
            else
                return string.Format("0{0}", pInt);
        }

        #endregion
    }
}
