/**
 * FUNCTIONAL_DESCRIPTION:
 * CREATE_BY: 尽际
 * CREATE_TIME: 2019/3/12 17:32
 * MODIFICATORY_DESCRIPTION:
 * MODIFY_BY:
 * MODIFICATORY_TIME:
 * VERSION：V1.0
 *
 *  1 2 4 7
 *  3 5 8
 *  6 9
 *  10
 */
public class DemoTest {
    public static void main(String[] args) {
        int x = 4;
        int y = 10;
        int lineHead;
        int result;
        int i;
        for(int m = 1; m <= x; m++){
            lineHead = m * (m + 1) / 2;
            result = lineHead;
            i = m;
            for(int n = lineHead; n <= y; n += i){
                System.out.print(result + " ");
                result = result + i;
                i++;
            }
            System.out.println();
        }
    }
}
