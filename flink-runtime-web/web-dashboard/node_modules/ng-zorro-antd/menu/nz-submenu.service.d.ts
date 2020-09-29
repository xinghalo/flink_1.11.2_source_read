import { BehaviorSubject, Subject } from 'rxjs';
import { NzDirectionVHIType } from '../core/types/direction';
import { NzMenuService } from './nz-menu.service';
export declare class NzSubmenuService {
    private nzHostSubmenuService;
    nzMenuService: NzMenuService;
    disabled: boolean;
    mode: NzDirectionVHIType;
    mode$: import("rxjs").Observable<NzDirectionVHIType>;
    level: number;
    level$: BehaviorSubject<number>;
    subMenuOpen$: BehaviorSubject<boolean>;
    open$: BehaviorSubject<boolean>;
    mouseEnterLeave$: Subject<boolean>;
    menuOpen$: import("rxjs").Observable<boolean>;
    setOpenState(value: boolean): void;
    onMenuItemClick(): void;
    setLevel(value: number): void;
    setMouseEnterState(value: boolean): void;
    constructor(nzHostSubmenuService: NzSubmenuService, nzMenuService: NzMenuService);
}
