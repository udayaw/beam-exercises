package common;

import java.io.File;

public class FSUtils {
    public static String OUTPUT_DIR = System.getProperty("user.dir") + "/build";


    public static String getOutputDirectory(Class cls){
        return FSUtils.OUTPUT_DIR + File.separator + cls.getPackage().getName() + File.separator + cls.getName() + "out/";
    }
}
