using System.IO;

namespace rCore
{
    public class IO
    {
        public static string LoadStructure(string structurePath)
        {
            if (File.Exists(structurePath)) { return File.ReadAllText(structurePath); }
            else { throw new FileNotFoundException("Specified file cannot be found.", structurePath); }
        }

    }
}
