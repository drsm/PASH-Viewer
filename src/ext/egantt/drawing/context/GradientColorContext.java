/*
 *
 */

package ext.egantt.drawing.context;

import com.egantt.awt.graphics.GraphicsContext;
import com.egantt.awt.paint.VerticalGradientPaint;
import java.awt.Color;
import java.awt.Paint;

import org.ash.util.Options;

public class GradientColorContext
{
    public static final class LocalColorContext
        implements GraphicsContext
    {

        public Object get(Object key, Object type)
        {
            return "Paint".equals(type) ? color : null;
        }

        public Paint getPaint()
        {
            return color;
        }

        protected final Paint color;

        public LocalColorContext(Color color)
        {
            this.color = new VerticalGradientPaint(color, color);
        }
    }


    public GradientColorContext()
    {
    }

    public static final GraphicsContext WHITE;
    public static final GraphicsContext LIGHT_GRAY;
    public static final GraphicsContext gray;
    public static final GraphicsContext GRAY;
    public static final GraphicsContext darkGray;
    public static final GraphicsContext DARK_GRAY;
    public static final GraphicsContext black;
    public static final GraphicsContext BLACK;
    public static final GraphicsContext red;
    public static final GraphicsContext RED;
    public static final GraphicsContext pink;
    public static final GraphicsContext PINK;
    public static final GraphicsContext orange;
    public static final GraphicsContext ORANGE;
    public static final GraphicsContext yellow;
    public static final GraphicsContext YELLOW;
    public static final GraphicsContext green;
    public static final GraphicsContext GREEN;
    public static final GraphicsContext magenta;
    public static final GraphicsContext MAGENTA;
    public static final GraphicsContext cyan;
    public static final GraphicsContext CYAN;
    public static final GraphicsContext blue;
    public static final GraphicsContext BLUE;
    
    public static final GraphicsContext CPU0;
    public static final GraphicsContext IO1;
    public static final GraphicsContext LOCK2;
    public static final GraphicsContext LWLOCK3;
    public static final GraphicsContext BUFFERPIN4;
    public static final GraphicsContext ACTIVITY5;
    public static final GraphicsContext EXTENSION6;
    public static final GraphicsContext CLIENT7;
    public static final GraphicsContext IPC8;
    public static final GraphicsContext TIMEOUT9;

    static 
    {
        WHITE = new LocalColorContext(Color.white);
        LIGHT_GRAY = new LocalColorContext(Color.lightGray);
        gray = new LocalColorContext(Color.gray);
        GRAY = gray;
        darkGray = new LocalColorContext(Color.darkGray);
        DARK_GRAY = darkGray;
        black = new LocalColorContext(Color.black);
        BLACK = black;
        red = new LocalColorContext(Color.red);
        RED = red;
        pink = new LocalColorContext(Color.pink);
        PINK = pink;
        orange = new LocalColorContext(Color.orange);
        ORANGE = orange;
        yellow = new LocalColorContext(Color.yellow);
        YELLOW = yellow;
        green = new LocalColorContext(Color.green);
        GREEN = green;
        magenta = new LocalColorContext(Color.magenta);
        MAGENTA = magenta;
        cyan = new LocalColorContext(Color.cyan);
        CYAN = cyan;
        blue = new LocalColorContext(Color.blue);
        BLUE = blue;

        CPU0 = new LocalColorContext(new Color(0,204,0));
        IO1 = new LocalColorContext(new Color(0,74,231));
        LOCK2 = new LocalColorContext(new Color(192,40,0));
        LWLOCK3 = new LocalColorContext(new Color(139,26,0));
        BUFFERPIN4 = new LocalColorContext(new Color(0,161,230));
        ACTIVITY5 = new LocalColorContext(new Color(255,165,0));
        EXTENSION6 = new LocalColorContext(new Color(0,123,20));
        CLIENT7 = new LocalColorContext(new Color(159,147,113));
        IPC8 = new LocalColorContext(new Color(240,110,170));
        TIMEOUT9 = new LocalColorContext(new Color(84,56,28));
    }
}
