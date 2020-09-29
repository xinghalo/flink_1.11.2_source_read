import { BehaviorSubject, Subject } from 'rxjs';
import { NzDirectionVHIType } from '../core/types/direction';
import { NzMenuItemDirective } from './nz-menu-item.directive';
export declare class NzMenuService {
    isInDropDown: boolean;
    menuItemClick$: Subject<NzMenuItemDirective>;
    theme$: Subject<{}>;
    mode$: BehaviorSubject<NzDirectionVHIType>;
    inlineIndent$: BehaviorSubject<number>;
    check$: import("rxjs").Observable<number | {} | "vertical" | "horizontal" | "inline">;
    theme: 'light' | 'dark';
    mode: NzDirectionVHIType;
    inlineIndent: number;
    menuOpen$: BehaviorSubject<boolean>;
    onMenuItemClick(menu: NzMenuItemDirective): void;
    setMode(mode: NzDirectionVHIType): void;
    setTheme(theme: 'light' | 'dark'): void;
    setInlineIndent(indent: number): void;
}
