/**
 * Animated Shiny Text — MagicUI (21st.dev community component)
 * Text with a moving light glare shimmer animation.
 * https://magicui.design/r/animated-shiny-text
 */
import type { CSSProperties, ReactNode } from "react";
import { cn } from "../../lib/utils";

interface AnimatedShinyTextProps {
  children: ReactNode;
  className?: string;
  shimmerWidth?: number;
}

export function AnimatedShinyText({
  children,
  className,
  shimmerWidth = 100,
}: AnimatedShinyTextProps) {
  return (
    <p
      style={
        {
          "--shiny-width": `${shimmerWidth}px`,
        } as CSSProperties
      }
      className={cn(
        "mx-auto max-w-md text-neutral-600/70",
        "animate-shiny-text bg-clip-text bg-no-repeat [background-position:0_0] [background-size:var(--shiny-width)_100%] [transition:background-position_1s_cubic-bezier(.6,.6,0,1)_infinite]",
        "bg-gradient-to-r from-transparent via-white/80 via-50% to-transparent",
        className
      )}
    >
      {children}
    </p>
  );
}
