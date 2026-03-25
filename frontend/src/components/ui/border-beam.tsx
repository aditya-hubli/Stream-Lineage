/**
 * Border Beam — MagicUI (21st.dev community component)
 * Animated gradient beam that travels around a container's border.
 * https://magicui.design/r/border-beam
 */
import { cn } from "../../lib/utils";

interface BorderBeamProps {
  className?: string;
  size?: number;
  duration?: number;
  delay?: number;
  colorFrom?: string;
  colorTo?: string;
  borderWidth?: number;
}

export function BorderBeam({
  className,
  size = 200,
  duration = 15,
  delay = 0,
  colorFrom = "#ffaa40",
  colorTo = "#9c40ff",
  borderWidth = 1.5,
}: BorderBeamProps) {
  return (
    <div
      style={
        {
          "--size": size,
          "--duration": `${duration}s`,
          "--delay": `${-delay}s`,
          "--color-from": colorFrom,
          "--color-to": colorTo,
          "--border-width": `${borderWidth}px`,
        } as React.CSSProperties
      }
      className={cn(
        "pointer-events-none absolute inset-0 rounded-[inherit]",
        "[border:calc(var(--border-width))_solid_transparent]",
        "[background:padding-box_transparent,border-box_conic-gradient(from_calc(270deg-(var(--size)*0.5deg)),transparent_0,var(--color-from)_calc(var(--size)*0.333deg),var(--color-to)_calc(var(--size)*0.666deg),transparent_calc(var(--size)*1deg))]",
        "animate-border-beam",
        "[mask:linear-gradient(white,white)_padding-box,linear-gradient(white,white)]",
        "[mask-composite:intersect]",
        className
      )}
    />
  );
}
