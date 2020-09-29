/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { ContentChildren, Directive, ElementRef, EventEmitter, Input, Optional, Output, QueryList, SkipSelf } from '@angular/core';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';
import { NzUpdateHostClassService } from '../core/services/update-host-class.service';
import { InputBoolean } from '../core/util/convert';
import { NzMenuDropdownService } from '../dropdown/nz-menu-dropdown.service';
import { NzMenuItemDirective } from './nz-menu-item.directive';
import { NzMenuMenuService } from './nz-menu-menu.service';
import { NzMenuService } from './nz-menu.service';
import { NzSubMenuComponent } from './nz-submenu.component';
/**
 * @param {?} dropService
 * @param {?} menuService
 * @return {?}
 */
export function NzMenuFactory(dropService, menuService) {
    return dropService ? dropService : menuService;
}
var NzMenuDirective = /** @class */ (function () {
    function NzMenuDirective(elementRef, nzMenuService, nzUpdateHostClassService) {
        this.elementRef = elementRef;
        this.nzMenuService = nzMenuService;
        this.nzUpdateHostClassService = nzUpdateHostClassService;
        this.destroy$ = new Subject();
        this.listOfOpenedNzSubMenuComponent = [];
        this.nzInlineIndent = 24;
        this.nzTheme = 'light';
        this.nzMode = 'vertical';
        this.nzInDropDown = false;
        this.nzInlineCollapsed = false;
        this.nzSelectable = !this.nzMenuService.isInDropDown;
        this.nzClick = new EventEmitter();
    }
    /**
     * @return {?}
     */
    NzMenuDirective.prototype.updateInlineCollapse = /**
     * @return {?}
     */
    function () {
        if (this.listOfNzMenuItemDirective) {
            if (this.nzInlineCollapsed) {
                this.listOfOpenedNzSubMenuComponent = this.listOfNzSubMenuComponent.filter((/**
                 * @param {?} submenu
                 * @return {?}
                 */
                function (submenu) { return submenu.nzOpen; }));
                this.listOfNzSubMenuComponent.forEach((/**
                 * @param {?} submenu
                 * @return {?}
                 */
                function (submenu) { return submenu.setOpenState(false); }));
                this.nzMode = 'vertical';
            }
            else {
                this.listOfOpenedNzSubMenuComponent.forEach((/**
                 * @param {?} submenu
                 * @return {?}
                 */
                function (submenu) { return submenu.setOpenState(true); }));
                this.listOfOpenedNzSubMenuComponent = [];
                this.nzMode = this.cacheMode;
            }
            this.nzMenuService.setMode(this.nzMode);
        }
    };
    /**
     * @return {?}
     */
    NzMenuDirective.prototype.setClassMap = /**
     * @return {?}
     */
    function () {
        var _a;
        /** @type {?} */
        var prefixName = this.nzMenuService.isInDropDown ? 'ant-dropdown-menu' : 'ant-menu';
        this.nzUpdateHostClassService.updateHostClass(this.elementRef.nativeElement, (_a = {},
            _a["" + prefixName] = true,
            _a[prefixName + "-root"] = true,
            _a[prefixName + "-" + this.nzTheme] = true,
            _a[prefixName + "-" + this.nzMode] = true,
            _a[prefixName + "-inline-collapsed"] = this.nzInlineCollapsed,
            _a));
    };
    /**
     * @return {?}
     */
    NzMenuDirective.prototype.ngOnInit = /**
     * @return {?}
     */
    function () {
        var _this = this;
        this.setClassMap();
        this.nzMenuService.menuItemClick$.pipe(takeUntil(this.destroy$)).subscribe((/**
         * @param {?} menu
         * @return {?}
         */
        function (menu) {
            _this.nzClick.emit(menu);
            if (_this.nzSelectable) {
                _this.listOfNzMenuItemDirective.forEach((/**
                 * @param {?} item
                 * @return {?}
                 */
                function (item) { return item.setSelectedState(item === menu); }));
            }
        }));
    };
    /**
     * @return {?}
     */
    NzMenuDirective.prototype.ngAfterContentInit = /**
     * @return {?}
     */
    function () {
        this.cacheMode = this.nzMode;
        this.updateInlineCollapse();
    };
    /**
     * @param {?} changes
     * @return {?}
     */
    NzMenuDirective.prototype.ngOnChanges = /**
     * @param {?} changes
     * @return {?}
     */
    function (changes) {
        if (changes.nzInlineCollapsed) {
            this.updateInlineCollapse();
        }
        if (changes.nzInlineIndent) {
            this.nzMenuService.setInlineIndent(this.nzInlineIndent);
        }
        if (changes.nzInDropDown) {
            this.nzMenuService.isInDropDown = this.nzInDropDown;
        }
        if (changes.nzTheme) {
            this.nzMenuService.setTheme(this.nzTheme);
        }
        if (changes.nzMode) {
            this.nzMenuService.setMode(this.nzMode);
            if (!changes.nzMode.isFirstChange() && this.listOfNzSubMenuComponent) {
                this.listOfNzSubMenuComponent.forEach((/**
                 * @param {?} submenu
                 * @return {?}
                 */
                function (submenu) { return submenu.setOpenState(false); }));
            }
        }
        if (changes.nzTheme || changes.nzMode || changes.nzInlineCollapsed) {
            this.setClassMap();
        }
    };
    /**
     * @return {?}
     */
    NzMenuDirective.prototype.ngOnDestroy = /**
     * @return {?}
     */
    function () {
        this.destroy$.next();
        this.destroy$.complete();
    };
    NzMenuDirective.decorators = [
        { type: Directive, args: [{
                    selector: '[nz-menu]',
                    providers: [
                        NzUpdateHostClassService,
                        NzMenuMenuService,
                        {
                            provide: NzMenuService,
                            useFactory: NzMenuFactory,
                            deps: [[new SkipSelf(), new Optional(), NzMenuDropdownService], NzMenuMenuService]
                        }
                    ]
                },] }
    ];
    /** @nocollapse */
    NzMenuDirective.ctorParameters = function () { return [
        { type: ElementRef },
        { type: NzMenuService },
        { type: NzUpdateHostClassService }
    ]; };
    NzMenuDirective.propDecorators = {
        listOfNzMenuItemDirective: [{ type: ContentChildren, args: [NzMenuItemDirective, { descendants: true },] }],
        listOfNzSubMenuComponent: [{ type: ContentChildren, args: [NzSubMenuComponent, { descendants: true },] }],
        nzInlineIndent: [{ type: Input }],
        nzTheme: [{ type: Input }],
        nzMode: [{ type: Input }],
        nzInDropDown: [{ type: Input }],
        nzInlineCollapsed: [{ type: Input }],
        nzSelectable: [{ type: Input }],
        nzClick: [{ type: Output }]
    };
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzMenuDirective.prototype, "nzInDropDown", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzMenuDirective.prototype, "nzInlineCollapsed", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzMenuDirective.prototype, "nzSelectable", void 0);
    return NzMenuDirective;
}());
export { NzMenuDirective };
if (false) {
    /**
     * @type {?}
     * @private
     */
    NzMenuDirective.prototype.destroy$;
    /**
     * @type {?}
     * @private
     */
    NzMenuDirective.prototype.cacheMode;
    /**
     * @type {?}
     * @private
     */
    NzMenuDirective.prototype.listOfOpenedNzSubMenuComponent;
    /** @type {?} */
    NzMenuDirective.prototype.listOfNzMenuItemDirective;
    /** @type {?} */
    NzMenuDirective.prototype.listOfNzSubMenuComponent;
    /** @type {?} */
    NzMenuDirective.prototype.nzInlineIndent;
    /** @type {?} */
    NzMenuDirective.prototype.nzTheme;
    /** @type {?} */
    NzMenuDirective.prototype.nzMode;
    /** @type {?} */
    NzMenuDirective.prototype.nzInDropDown;
    /** @type {?} */
    NzMenuDirective.prototype.nzInlineCollapsed;
    /** @type {?} */
    NzMenuDirective.prototype.nzSelectable;
    /** @type {?} */
    NzMenuDirective.prototype.nzClick;
    /** @type {?} */
    NzMenuDirective.prototype.elementRef;
    /**
     * @type {?}
     * @private
     */
    NzMenuDirective.prototype.nzMenuService;
    /**
     * @type {?}
     * @private
     */
    NzMenuDirective.prototype.nzUpdateHostClassService;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotbWVudS5kaXJlY3RpdmUuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsibWVudS9uei1tZW51LmRpcmVjdGl2ZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFFTCxlQUFlLEVBQ2YsU0FBUyxFQUNULFVBQVUsRUFDVixZQUFZLEVBQ1osS0FBSyxFQUlMLFFBQVEsRUFDUixNQUFNLEVBQ04sU0FBUyxFQUVULFFBQVEsRUFDVCxNQUFNLGVBQWUsQ0FBQztBQUN2QixPQUFPLEVBQUUsT0FBTyxFQUFFLE1BQU0sTUFBTSxDQUFDO0FBQy9CLE9BQU8sRUFBRSxTQUFTLEVBQUUsTUFBTSxnQkFBZ0IsQ0FBQztBQUMzQyxPQUFPLEVBQUUsd0JBQXdCLEVBQUUsTUFBTSw0Q0FBNEMsQ0FBQztBQUV0RixPQUFPLEVBQUUsWUFBWSxFQUFFLE1BQU0sc0JBQXNCLENBQUM7QUFDcEQsT0FBTyxFQUFFLHFCQUFxQixFQUFFLE1BQU0sc0NBQXNDLENBQUM7QUFDN0UsT0FBTyxFQUFFLG1CQUFtQixFQUFFLE1BQU0sMEJBQTBCLENBQUM7QUFDL0QsT0FBTyxFQUFFLGlCQUFpQixFQUFFLE1BQU0sd0JBQXdCLENBQUM7QUFDM0QsT0FBTyxFQUFFLGFBQWEsRUFBRSxNQUFNLG1CQUFtQixDQUFDO0FBQ2xELE9BQU8sRUFBRSxrQkFBa0IsRUFBRSxNQUFNLHdCQUF3QixDQUFDOzs7Ozs7QUFFNUQsTUFBTSxVQUFVLGFBQWEsQ0FBQyxXQUFrQyxFQUFFLFdBQThCO0lBQzlGLE9BQU8sV0FBVyxDQUFDLENBQUMsQ0FBQyxXQUFXLENBQUMsQ0FBQyxDQUFDLFdBQVcsQ0FBQztBQUNqRCxDQUFDO0FBRUQ7SUFzREUseUJBQ1MsVUFBc0IsRUFDckIsYUFBNEIsRUFDNUIsd0JBQWtEO1FBRm5ELGVBQVUsR0FBVixVQUFVLENBQVk7UUFDckIsa0JBQWEsR0FBYixhQUFhLENBQWU7UUFDNUIsNkJBQXdCLEdBQXhC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Ozs7O0lBQzdCLHdDQUFvQzs7Ozs7SUFDcEMsbURBQTBEIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHtcbiAgQWZ0ZXJDb250ZW50SW5pdCxcbiAgQ29udGVudENoaWxkcmVuLFxuICBEaXJlY3RpdmUsXG4gIEVsZW1lbnRSZWYsXG4gIEV2ZW50RW1pdHRlcixcbiAgSW5wdXQsXG4gIE9uQ2hhbmdlcyxcbiAgT25EZXN0cm95LFxuICBPbkluaXQsXG4gIE9wdGlvbmFsLFxuICBPdXRwdXQsXG4gIFF1ZXJ5TGlzdCxcbiAgU2ltcGxlQ2hhbmdlcyxcbiAgU2tpcFNlbGZcbn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgeyBTdWJqZWN0IH0gZnJvbSAncnhqcyc7XG5pbXBvcnQgeyB0YWtlVW50aWwgfSBmcm9tICdyeGpzL29wZXJhdG9ycyc7XG5pbXBvcnQgeyBOelVwZGF0ZUhvc3RDbGFzc1NlcnZpY2UgfSBmcm9tICcuLi9jb3JlL3NlcnZpY2VzL3VwZGF0ZS1ob3N0LWNsYXNzLnNlcnZpY2UnO1xuaW1wb3J0IHsgTnpEaXJlY3Rpb25WSElUeXBlIH0gZnJvbSAnLi4vY29yZS90eXBlcy9kaXJlY3Rpb24nO1xuaW1wb3J0IHsgSW5wdXRCb29sZWFuIH0gZnJvbSAnLi4vY29yZS91dGlsL2NvbnZlcnQnO1xuaW1wb3J0IHsgTnpNZW51RHJvcGRvd25TZXJ2aWNlIH0gZnJvbSAnLi4vZHJvcGRvd24vbnotbWVudS1kcm9wZG93bi5zZXJ2aWNlJztcbmltcG9ydCB7IE56TWVudUl0ZW1EaXJlY3RpdmUgfSBmcm9tICcuL256LW1lbnUtaXRlbS5kaXJlY3RpdmUnO1xuaW1wb3J0IHsgTnpNZW51TWVudVNlcnZpY2UgfSBmcm9tICcuL256LW1lbnUtbWVudS5zZXJ2aWNlJztcbmltcG9ydCB7IE56TWVudVNlcnZpY2UgfSBmcm9tICcuL256LW1lbnUuc2VydmljZSc7XG5pbXBvcnQgeyBOelN1Yk1lbnVDb21wb25lbnQgfSBmcm9tICcuL256LXN1Ym1lbnUuY29tcG9uZW50JztcblxuZXhwb3J0IGZ1bmN0aW9uIE56TWVudUZhY3RvcnkoZHJvcFNlcnZpY2U6IE56TWVudURyb3Bkb3duU2VydmljZSwgbWVudVNlcnZpY2U6IE56TWVudU1lbnVTZXJ2aWNlKTogTnpNZW51U2VydmljZSB7XG4gIHJldHVybiBkcm9wU2VydmljZSA/IGRyb3BTZXJ2aWNlIDogbWVudVNlcnZpY2U7XG59XG5cbkBEaXJlY3RpdmUoe1xuICBzZWxlY3RvcjogJ1tuei1tZW51XScsXG4gIHByb3ZpZGVyczogW1xuICAgIE56VXBkYXRlSG9zdENsYXNzU2VydmljZSxcbiAgICBOek1lbnVNZW51U2VydmljZSxcbiAgICB7XG4gICAgICBwcm92aWRlOiBOek1lbnVTZXJ2aWNlLFxuICAgICAgdXNlRmFjdG9yeTogTnpNZW51RmFjdG9yeSxcbiAgICAgIGRlcHM6IFtbbmV3IFNraXBTZWxmKCksIG5ldyBPcHRpb25hbCgpLCBOek1lbnVEcm9wZG93blNlcnZpY2VdLCBOek1lbnVNZW51U2VydmljZV1cbiAgICB9XG4gIF1cbn0pXG5leHBvcnQgY2xhc3MgTnpNZW51RGlyZWN0aXZlIGltcGxlbWVudHMgQWZ0ZXJDb250ZW50SW5pdCwgT25Jbml0LCBPbkNoYW5nZXMsIE9uRGVzdHJveSB7XG4gIHByaXZhdGUgZGVzdHJveSQgPSBuZXcgU3ViamVjdCgpO1xuICBwcml2YXRlIGNhY2hlTW9kZTogTnpEaXJlY3Rpb25WSElUeXBlO1xuICBwcml2YXRlIGxpc3RPZk9wZW5lZE56U3ViTWVudUNvbXBvbmVudDogTnpTdWJNZW51Q29tcG9uZW50W10gPSBbXTtcbiAgQENvbnRlbnRDaGlsZHJlbihOek1lbnVJdGVtRGlyZWN0aXZlLCB7IGRlc2NlbmRhbnRzOiB0cnVlIH0pIGxpc3RPZk56TWVudUl0ZW1EaXJlY3RpdmU6IFF1ZXJ5TGlzdDxcbiAgICBOek1lbnVJdGVtRGlyZWN0aXZlXG4gID47XG4gIEBDb250ZW50Q2hpbGRyZW4oTnpTdWJNZW51Q29tcG9uZW50LCB7IGRlc2NlbmRhbnRzOiB0cnVlIH0pIGxpc3RPZk56U3ViTWVudUNvbXBvbmVudDogUXVlcnlMaXN0PE56U3ViTWVudUNvbXBvbmVudD47XG4gIEBJbnB1dCgpIG56SW5saW5lSW5kZW50ID0gMjQ7XG4gIEBJbnB1dCgpIG56VGhlbWU6ICdsaWdodCcgfCAnZGFyaycgPSAnbGlnaHQnO1xuICBASW5wdXQoKSBuek1vZGU6IE56RGlyZWN0aW9uVkhJVHlwZSA9ICd2ZXJ0aWNhbCc7XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuekluRHJvcERvd24gPSBmYWxzZTtcbiAgQElucHV0KCkgQElucHV0Qm9vbGVhbigpIG56SW5saW5lQ29sbGFwc2VkID0gZmFsc2U7XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuelNlbGVjdGFibGUgPSAhdGhpcy5uek1lbnVTZXJ2aWNlLmlzSW5Ecm9wRG93bjtcbiAgQE91dHB1dCgpIHJlYWRvbmx5IG56Q2xpY2sgPSBuZXcgRXZlbnRFbWl0dGVyPE56TWVudUl0ZW1EaXJlY3RpdmU+KCk7XG5cbiAgdXBkYXRlSW5saW5lQ29sbGFwc2UoKTogdm9pZCB7XG4gICAgaWYgKHRoaXMubGlzdE9mTnpNZW51SXRlbURpcmVjdGl2ZSkge1xuICAgICAgaWYgKHRoaXMubnpJbmxpbmVDb2xsYXBzZWQpIHtcbiAgICAgICAgdGhpcy5saXN0T2ZPcGVuZWROelN1Yk1lbnVDb21wb25lbnQgPSB0aGlzLmxpc3RPZk56U3ViTWVudUNvbXBvbmVudC5maWx0ZXIoc3VibWVudSA9PiBzdWJtZW51Lm56T3Blbik7XG4gICAgICAgIHRoaXMubGlzdE9mTnpTdWJNZW51Q29tcG9uZW50LmZvckVhY2goc3VibWVudSA9PiBzdWJtZW51LnNldE9wZW5TdGF0ZShmYWxzZSkpO1xuICAgICAgICB0aGlzLm56TW9kZSA9ICd2ZXJ0aWNhbCc7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICB0aGlzLmxpc3RPZk9wZW5lZE56U3ViTWVudUNvbXBvbmVudC5mb3JFYWNoKHN1Ym1lbnUgPT4gc3VibWVudS5zZXRPcGVuU3RhdGUodHJ1ZSkpO1xuICAgICAgICB0aGlzLmxpc3RPZk9wZW5lZE56U3ViTWVudUNvbXBvbmVudCA9IFtdO1xuICAgICAgICB0aGlzLm56TW9kZSA9IHRoaXMuY2FjaGVNb2RlO1xuICAgICAgfVxuICAgICAgdGhpcy5uek1lbnVTZXJ2aWNlLnNldE1vZGUodGhpcy5uek1vZGUpO1xuICAgIH1cbiAgfVxuXG4gIHNldENsYXNzTWFwKCk6IHZvaWQge1xuICAgIGNvbnN0IHByZWZpeE5hbWUgPSB0aGlzLm56TWVudVNlcnZpY2UuaXNJbkRyb3BEb3duID8gJ2FudC1kcm9wZG93bi1tZW51JyA6ICdhbnQtbWVudSc7XG4gICAgdGhpcy5uelVwZGF0ZUhvc3RDbGFzc1NlcnZpY2UudXBkYXRlSG9zdENsYXNzKHRoaXMuZWxlbWVudFJlZi5uYXRpdmVFbGVtZW50LCB7XG4gICAgICBbYCR7cHJlZml4TmFtZX1gXTogdHJ1ZSxcbiAgICAgIFtgJHtwcmVmaXhOYW1lfS1yb290YF06IHRydWUsXG4gICAgICBbYCR7cHJlZml4TmFtZX0tJHt0aGlzLm56VGhlbWV9YF06IHRydWUsXG4gICAgICBbYCR7cHJlZml4TmFtZX0tJHt0aGlzLm56TW9kZX1gXTogdHJ1ZSxcbiAgICAgIFtgJHtwcmVmaXhOYW1lfS1pbmxpbmUtY29sbGFwc2VkYF06IHRoaXMubnpJbmxpbmVDb2xsYXBzZWRcbiAgICB9KTtcbiAgfVxuXG4gIGNvbnN0cnVjdG9yKFxuICAgIHB1YmxpYyBlbGVtZW50UmVmOiBFbGVtZW50UmVmLFxuICAgIHByaXZhdGUgbnpNZW51U2VydmljZTogTnpNZW51U2VydmljZSxcbiAgICBwcml2YXRlIG56VXBkYXRlSG9zdENsYXNzU2VydmljZTogTnpVcGRhdGVIb3N0Q2xhc3NTZXJ2aWNlXG4gICkge31cblxuICBuZ09uSW5pdCgpOiB2b2lkIHtcbiAgICB0aGlzLnNldENsYXNzTWFwKCk7XG4gICAgdGhpcy5uek1lbnVTZXJ2aWNlLm1lbnVJdGVtQ2xpY2skLnBpcGUodGFrZVVudGlsKHRoaXMuZGVzdHJveSQpKS5zdWJzY3JpYmUobWVudSA9PiB7XG4gICAgICB0aGlzLm56Q2xpY2suZW1pdChtZW51KTtcbiAgICAgIGlmICh0aGlzLm56U2VsZWN0YWJsZSkge1xuICAgICAgICB0aGlzLmxpc3RPZk56TWVudUl0ZW1EaXJlY3RpdmUuZm9yRWFjaChpdGVtID0+IGl0ZW0uc2V0U2VsZWN0ZWRTdGF0ZShpdGVtID09PSBtZW51KSk7XG4gICAgICB9XG4gICAgfSk7XG4gIH1cblxuICBuZ0FmdGVyQ29udGVudEluaXQoKTogdm9pZCB7XG4gICAgdGhpcy5jYWNoZU1vZGUgPSB0aGlzLm56TW9kZTtcbiAgICB0aGlzLnVwZGF0ZUlubGluZUNvbGxhcHNlKCk7XG4gIH1cblxuICBuZ09uQ2hhbmdlcyhjaGFuZ2VzOiBTaW1wbGVDaGFuZ2VzKTogdm9pZCB7XG4gICAgaWYgKGNoYW5nZXMubnpJbmxpbmVDb2xsYXBzZWQpIHtcbiAgICAgIHRoaXMudXBkYXRlSW5saW5lQ29sbGFwc2UoKTtcbiAgICB9XG4gICAgaWYgKGNoYW5nZXMubnpJbmxpbmVJbmRlbnQpIHtcbiAgICAgIHRoaXMubnpNZW51U2VydmljZS5zZXRJbmxpbmVJbmRlbnQodGhpcy5ueklubGluZUluZGVudCk7XG4gICAgfVxuICAgIGlmIChjaGFuZ2VzLm56SW5Ecm9wRG93bikge1xuICAgICAgdGhpcy5uek1lbnVTZXJ2aWNlLmlzSW5Ecm9wRG93biA9IHRoaXMubnpJbkRyb3BEb3duO1xuICAgIH1cbiAgICBpZiAoY2hhbmdlcy5uelRoZW1lKSB7XG4gICAgICB0aGlzLm56TWVudVNlcnZpY2Uuc2V0VGhlbWUodGhpcy5uelRoZW1lKTtcbiAgICB9XG4gICAgaWYgKGNoYW5nZXMubnpNb2RlKSB7XG4gICAgICB0aGlzLm56TWVudVNlcnZpY2Uuc2V0TW9kZSh0aGlzLm56TW9kZSk7XG4gICAgICBpZiAoIWNoYW5nZXMubnpNb2RlLmlzRmlyc3RDaGFuZ2UoKSAmJiB0aGlzLmxpc3RPZk56U3ViTWVudUNvbXBvbmVudCkge1xuICAgICAgICB0aGlzLmxpc3RPZk56U3ViTWVudUNvbXBvbmVudC5mb3JFYWNoKHN1Ym1lbnUgPT4gc3VibWVudS5zZXRPcGVuU3RhdGUoZmFsc2UpKTtcbiAgICAgIH1cbiAgICB9XG4gICAgaWYgKGNoYW5nZXMubnpUaGVtZSB8fCBjaGFuZ2VzLm56TW9kZSB8fCBjaGFuZ2VzLm56SW5saW5lQ29sbGFwc2VkKSB7XG4gICAgICB0aGlzLnNldENsYXNzTWFwKCk7XG4gICAgfVxuICB9XG5cbiAgbmdPbkRlc3Ryb3koKTogdm9pZCB7XG4gICAgdGhpcy5kZXN0cm95JC5uZXh0KCk7XG4gICAgdGhpcy5kZXN0cm95JC5jb21wbGV0ZSgpO1xuICB9XG59XG4iXX0=