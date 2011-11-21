
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;

public class Test {

    public static void main(String[] args) {
        WebDriver driver = new FirefoxDriver();
        try {
            driver.get("http://www.google.com");
            WebElement element = driver.findElement(By.name("q"));
            element.sendKeys("MXUnit");
            element.submit();
        } finally {
            driver.quit();
        }
    }

}