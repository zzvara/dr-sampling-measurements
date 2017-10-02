package hu.sztaki.sm;

import com.jogamp.opengl.GL;
import com.jogamp.opengl.glu.GLU;
import com.jogamp.opengl.util.awt.TextRenderer;
import org.jzy3d.maths.BoundingBox3d;
import org.jzy3d.maths.Coord2d;
import org.jzy3d.maths.Coord3d;
import org.jzy3d.plot3d.rendering.view.Camera;
import org.jzy3d.plot3d.text.AbstractTextRenderer;
import org.jzy3d.plot3d.text.ITextRenderer;
import org.jzy3d.plot3d.text.align.Halign;
import org.jzy3d.plot3d.text.align.Valign;
import org.jzy3d.plot3d.text.renderers.jogl.DefaultTextStyle;
import org.jzy3d.plot3d.text.renderers.jogl.ITextStyle;

import java.awt.*;
import java.awt.geom.Rectangle2D;

public class FunnyTextRenderer extends AbstractTextRenderer implements ITextRenderer {
    protected boolean LAYOUT;
    protected Font font;
    protected TextRenderer.RenderDelegate style;
    protected TextRenderer renderer;

    public FunnyTextRenderer(Font daFont) {
        this(new DefaultTextStyle(Color.BLUE), daFont, false,
                new TextRenderer(daFont, true, true, new DefaultTextStyle(Color.BLUE)));
    }

    public FunnyTextRenderer(ITextStyle style, Font daFont, boolean layout, TextRenderer renderer) {
        this.LAYOUT = layout;
        this.font = daFont;
        this.renderer = renderer;
        this.style = style;
    }

    public void drawSimpleText(GL gl, GLU glu, Camera cam, String s, Coord3d position, org.jzy3d.colors.Color color) {
        this.renderer.begin3DRendering();
        this.renderer.draw3D(s, position.x, position.y, position.z, 0.01F);
        this.renderer.flush();
        this.renderer.end3DRendering();
    }

    public BoundingBox3d drawText(GL gl, GLU glu, Camera cam, String s, Coord3d position, Halign halign, Valign valign, org.jzy3d.colors.Color color, Coord2d screenOffset, Coord3d sceneOffset) {
        this.renderer.begin3DRendering();
        if(this.LAYOUT) {
            Rectangle2D real = this.style.getBounds(s, this.font, this.renderer.getFontRenderContext());
            Coord3d left2d = cam.modelToScreen(gl, glu, position);
            Coord2d right2d = new Coord2d(left2d.x + (float)real.getWidth(), left2d.y + (float)real.getHeight());
            Coord3d right3d = cam.screenToModel(gl, glu, new Coord3d(right2d, 0.0F));
            Coord3d offset3d = right3d.sub(position).div(2.0F);
            Coord3d real1 = position.add(sceneOffset).sub(offset3d);
            this.renderer.draw3D(s, real1.x, real1.y, real1.z, 1.0F);
        } else {
            Coord3d real2 = position.add(sceneOffset);
            this.renderer.draw3D(s, real2.x, real2.y, real2.z, 1.0F);
        }

        this.renderer.flush();
        this.renderer.end3DRendering();
        return null;
    }
}
