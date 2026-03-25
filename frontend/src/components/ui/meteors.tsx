/**
 * Meteors — Aceternity UI (21st.dev community component)
 * Animated falling meteor streaks for card decoration.
 * https://ui.aceternity.com/components/meteors
 */
import { cn } from "../../lib/utils";

interface MeteorsProps {
  number?: number;
  className?: string;
}

export function Meteors({ number = 20, className }: MeteorsProps) {
  const meteors = [...Array(number)].map((_, idx) => ({
    id: idx,
    left: `${Math.floor(Math.random() * 100)}%`,
    animationDelay: `${Math.random() * 1 + 0.2}s`,
    animationDuration: `${Math.floor(Math.random() * 8 + 2)}s`,
  }));

  return (
    <>
      {meteors.map((meteor) => (
        <span
          key={meteor.id}
          className={cn(
            "animate-meteor-effect absolute h-0.5 w-0.5 rotate-[215deg] rounded-[9999px] bg-slate-500 shadow-[0_0_0_1px_#ffffff10]",
            "before:absolute before:top-1/2 before:h-px before:w-[50px] before:-translate-y-1/2 before:transform before:bg-gradient-to-r before:from-[#64748b] before:to-transparent before:content-['']",
            className
          )}
          style={{
            top: "-5%",
            left: meteor.left,
            animationDelay: meteor.animationDelay,
            animationDuration: meteor.animationDuration,
          }}
        />
      ))}
    </>
  );
}
