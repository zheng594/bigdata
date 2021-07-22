import java.util.HashMap;

public class Algorithm {
    public static void main(String[] args) {
        String str = "3131243423432333";
        HashMap<String,String> map = new HashMap<>();

        for(int i=0;i<str.length();i++){
            System.out.println(i+":"+map.hashCode());
        }
    }
}
