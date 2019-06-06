/**
 * @Author lzc
 * @Date 2019-06-06 15:45
 */
public class MyRe {
    public static void main(String[] args) {
//        String s = "12345";
//        boolean r = s.matches("\\d{3,5}");
//        String phone = "14603071634";
//        boolean r = phone.matches("1[356789]\\d{9}");
        // () 捕获组
//        boolean r = "abab".matches("(ab){2}");
//        boolean r = "abc@sohu.com".matches("[a-zA-Z_]\\w{2,14}@\\w+\\.(com|cn|org|edu|com\\.cn)");
//        System.out.println(r);
//        String s = "abc1234dlfjl234";
//        String r = s.replaceAll("\\d+", "*");  //  \\W+

//        String s = "开开心心";
//        String r = s.replaceAll("(.)\\1+", "$1");
//        System.out.println(r);

        String s = "..10.1.2.3.";
        String[] split = s.split("\\.");
        System.out.println(split.length);
//        for (String s1 : split) {
//            System.out.println(s1);
        }

//    }
}
/*
[abc] 匹配要么a要么b要么c
[a-z] 匹配小写字母
[^abc] 在方括号中的 ^ 表示非
\d  表示数字(digital)   === [0-9]
\D   表示非数字
\w  表示单词字符(word)  数字字母下划线
\W  非单词字符串
\s  空白字符(space) 空格 \n \r \t
\S  非空白字符
.   表示任意字符(除了换行, \r)

数量词:
    a? 0 个或1个 a  a{0,1}
    a* 0个或多个a   a{0,}
    a+ 1个或多个a   a{1,}
    a{m} 正好m 个a
    a{m,} 至少m个a
    a{m,n} 至少m个a, 至多n个

() 捕获组

^a 表示行的开头
b$ 表示行的结尾


正则表达式:
    一个工具, 用来处理字符串的强大的工具

    匹配字符串

    字符串当成一个贪官,  正则相当于法律

java中有两个核心支持正则类:

Pattern  模式 把字符串编译成正则
Matcher  匹配器  使用匹配器去匹配

字符串中已经提供了4个方法来直接使用正则:

matches
replaceAll
replaceFirst
split


 */
