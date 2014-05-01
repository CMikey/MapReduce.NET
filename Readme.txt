Because of the SQLite output, Mapreduce.NET is built for 32 bit output as IIExpress currently cannot host 64 bit applications (so the reference to SQLite must be the 32 bit).
If you want to compile to 64 bit, either use full IIS or don't use the remoting part.

Inputs:

MapReduce.NET.Input.TextfileLineInput
	public DateTime? CreationDateAtLeast  { get; set; }
    public DateTime? CreationDateAtMost { get; set; }

Outputs:

MapReduce.NET.Output.ConsoleOutput

MapReduce.NET.Output.CSVJSONOutput

MapReduce.NET.Output.SQLiteProtoBufOutput

MapReduce.NET.Output.SQLiteOutput