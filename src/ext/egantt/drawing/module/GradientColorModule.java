/*
 *
 */

package ext.egantt.drawing.module;

import java.awt.Color;
import java.awt.Paint;
import java.util.Iterator;
import java.util.List;

import org.ash.util.Options;

import com.egantt.awt.graphics.GraphicsContext;
import com.egantt.awt.paint.VerticalGradientPaint;
import com.egantt.model.drawing.ContextResources;
import com.egantt.model.drawing.DrawingContext;
import ext.egantt.drawing.DrawingModule;
import ext.egantt.drawing.context.GradientColorContext;
import ext.egantt.drawing.context.GradientColorContext.LocalColorContext;

public class GradientColorModule
    implements DrawingModule
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

    public GradientColorModule()
    {
    }

    public void initialise(DrawingContext attributes) {
    	  attributes.put("GradientColorContext.BLACK", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.BLACK);
          attributes.put("GradientColorContext.BLUE", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.BLUE);
          attributes.put("GradientColorContext.CYAN", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.CYAN);
          attributes.put("GradientColorContext.DARK_GRAY", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.DARK_GRAY);
          attributes.put("GradientColorContext.GRAY", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.GRAY);
          attributes.put("GradientColorContext.GREEN", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.GREEN);
          attributes.put("GradientColorContext.LIGHT_GRAY", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.LIGHT_GRAY);
          attributes.put("GradientColorContext.MAGENTA", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.MAGENTA);
          attributes.put("GradientColorContext.ORANGE", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.ORANGE);
          attributes.put("GradientColorContext.PINK", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.PINK);
          attributes.put("GradientColorContext.RED", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.RED);
          attributes.put("GradientColorContext.WHITE", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.WHITE);
          attributes.put("GradientColorContext.YELLOW", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.YELLOW);
          
          attributes.put("GradientColorContext.CPU0", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.CPU0);
          attributes.put("GradientColorContext.IO1", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.IO1);
          attributes.put("GradientColorContext.LOCK2", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.LOCK2);
          attributes.put("GradientColorContext.LWLOCK3", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.LWLOCK3);
          attributes.put("GradientColorContext.BUFFERPIN4", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.BUFFERPIN4);
          attributes.put("GradientColorContext.ACTIVITY5", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.ACTIVITY5);
          attributes.put("GradientColorContext.EXTENSION6", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.EXTENSION6);
          attributes.put("GradientColorContext.CLIENT7", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.CLIENT7);
          attributes.put("GradientColorContext.IPC8", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.IPC8);
          attributes.put("GradientColorContext.TIMEOUT9", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.TIMEOUT9);
	}
    
    public void initialise(DrawingContext attributes, List eventList)
    {
        attributes.put("GradientColorContext.BLACK", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.BLACK);
        attributes.put("GradientColorContext.BLUE", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.BLUE);
        attributes.put("GradientColorContext.CYAN", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.CYAN);
        attributes.put("GradientColorContext.DARK_GRAY", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.DARK_GRAY);
        attributes.put("GradientColorContext.GRAY", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.GRAY);
        attributes.put("GradientColorContext.GREEN", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.GREEN);
        attributes.put("GradientColorContext.LIGHT_GRAY", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.LIGHT_GRAY);
        attributes.put("GradientColorContext.MAGENTA", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.MAGENTA);
        attributes.put("GradientColorContext.ORANGE", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.ORANGE);
        attributes.put("GradientColorContext.PINK", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.PINK);
        attributes.put("GradientColorContext.RED", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.RED);
        attributes.put("GradientColorContext.WHITE", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.WHITE);
        attributes.put("GradientColorContext.YELLOW", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.YELLOW);
        
        attributes.put("GradientColorContext.CPU0", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.CPU0);
        attributes.put("GradientColorContext.IO1", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.IO1);
        attributes.put("GradientColorContext.LOCK2", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.LOCK2);
        attributes.put("GradientColorContext.LWLOCK3", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.LWLOCK3);
        attributes.put("GradientColorContext.BUFFERPIN4", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.BUFFERPIN4);
        attributes.put("GradientColorContext.ACTIVITY5", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.ACTIVITY5);
        attributes.put("GradientColorContext.EXTENSION6", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.EXTENSION6);
        attributes.put("GradientColorContext.CLIENT7", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.CLIENT7);
        attributes.put("GradientColorContext.IPC8", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.IPC8);
        attributes.put("GradientColorContext.TIMEOUT9", ContextResources.GRAPHICS_CONTEXT, GradientColorContext.TIMEOUT9);
        
        if (eventList != null){
        	Iterator iterEvent = eventList.iterator();
			while (iterEvent.hasNext()) {
				String eventName = (String) iterEvent.next();
				attributes.put(eventName,
								ContextResources.GRAPHICS_CONTEXT, 
									new LocalColorContext(Options.getInstance().getColor(eventName)));
			}
        }
        
    }

    public void terminate(DrawingContext attributes)
    {
        attributes.put("GradientColorContext.BLACK", ContextResources.GRAPHICS_CONTEXT, null);
        attributes.put("GradientColorContext.BLUE", ContextResources.GRAPHICS_CONTEXT, null);
        attributes.put("GradientColorContext.CYAN", ContextResources.GRAPHICS_CONTEXT, null);
        attributes.put("GradientColorContext.DARK_GRAY", ContextResources.GRAPHICS_CONTEXT, null);
        attributes.put("GradientColorContext.GRAY", ContextResources.GRAPHICS_CONTEXT, null);
        attributes.put("GradientColorContext.GREEN", ContextResources.GRAPHICS_CONTEXT, null);
        attributes.put("GradientColorContext.LIGHT_GRAY", ContextResources.GRAPHICS_CONTEXT, null);
        attributes.put("GradientColorContext.MAGENTA", ContextResources.GRAPHICS_CONTEXT, null);
        attributes.put("GradientColorContext.ORANGE", ContextResources.GRAPHICS_CONTEXT, null);
        attributes.put("GradientColorContext.PINK", ContextResources.GRAPHICS_CONTEXT, null);
        attributes.put("GradientColorContext.RED", ContextResources.GRAPHICS_CONTEXT, null);
        attributes.put("GradientColorContext.WHITE", ContextResources.GRAPHICS_CONTEXT, null);
        attributes.put("GradientColorContext.YELLOW", ContextResources.GRAPHICS_CONTEXT, null);
        
        attributes.put("GradientColorContext.CPU0", ContextResources.GRAPHICS_CONTEXT, null);
        attributes.put("GradientColorContext.IO1", ContextResources.GRAPHICS_CONTEXT, null);
        attributes.put("GradientColorContext.LOCK2", ContextResources.GRAPHICS_CONTEXT, null);
        attributes.put("GradientColorContext.LWLOCK3", ContextResources.GRAPHICS_CONTEXT, null);
        attributes.put("GradientColorContext.BUFFERPIN4", ContextResources.GRAPHICS_CONTEXT, null);
        attributes.put("GradientColorContext.ACTIVITY5", ContextResources.GRAPHICS_CONTEXT, null);
        attributes.put("GradientColorContext.EXTENSION6", ContextResources.GRAPHICS_CONTEXT, null);
        attributes.put("GradientColorContext.CLIENT7", ContextResources.GRAPHICS_CONTEXT, null);
        attributes.put("GradientColorContext.IPC8", ContextResources.GRAPHICS_CONTEXT, null);
        attributes.put("GradientColorContext.TIMEOUT9", ContextResources.GRAPHICS_CONTEXT, null);
    }

    public static final String BLACK_GRADIENT_CONTEXT = "GradientColorContext.BLACK";
    public static final String BLUE_GRADIENT_CONTEXT = "GradientColorContext.BLUE";
    public static final String CYAN_GRADIENT_CONTEXT = "GradientColorContext.CYAN";
    public static final String DARK_GRAY_GRADIENT_CONTEXT = "GradientColorContext.DARK_GRAY";
    public static final String GRAY_GRADIENT_CONTEXT = "GradientColorContext.GRAY";
    public static final String GREEN_GRADIENT_CONTEXT = "GradientColorContext.GREEN";
    public static final String LIGHT_GRAY_GRADIENT_CONTEXT = "GradientColorContext.LIGHT_GRAY";
    public static final String MAGENTA_GRADIENT_CONTEXT = "GradientColorContext.MAGENTA";
    public static final String ORANGE_GRADIENT_CONTEXT = "GradientColorContext.ORANGE";
    public static final String PINK_GRADIENT_CONTEXT = "GradientColorContext.PINK";
    public static final String RED_GRADIENT_CONTEXT = "GradientColorContext.RED";
    public static final String WHITE_GRADIENT_CONTEXT = "GradientColorContext.WHITE";
    public static final String YELLOW_GRADIENT_CONTEXT = "GradientColorContext.YELLOW";
    
    public static final String CPU0_GRADIENT_CONTEXT = "GradientColorContext.CPU0";
    public static final String IO1_GRADIENT_CONTEXT = "GradientColorContext.IO1";
    public static final String LOCK2_GRADIENT_CONTEXT = "GradientColorContext.LOCK2";
    public static final String LWLOCK3_GRADIENT_CONTEXT = "GradientColorContext.LWLOCK3";
    public static final String BUFFERPIN4_GRADIENT_CONTEXT = "GradientColorContext.BUFFERPIN4";
    public static final String ACTIVITY5_GRADIENT_CONTEXT = "GradientColorContext.ACTIVITY5";
    public static final String EXTENSION6_GRADIENT_CONTEXT = "GradientColorContext.EXTENSION6";
    public static final String CLIENT7_GRADIENT_CONTEXT = "GradientColorContext.CLIENT7";
    public static final String IPC8_GRADIENT_CONTEXT = "GradientColorContext.IPC8";
    public static final String TIMEOUT9_GRADIENT_CONTEXT = "GradientColorContext.TIMEOUT9";
}
