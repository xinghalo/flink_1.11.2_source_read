/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { NzAvatarModule } from '../avatar/nz-avatar.module';
import { NzAddOnModule } from '../core/addon/addon.module';
import { NzEmptyModule } from '../empty/nz-empty.module';
import { NzGridModule } from '../grid/nz-grid.module';
import { NzSpinModule } from '../spin/nz-spin.module';
import { NzListItemMetaComponent } from './nz-list-item-meta.component';
import { NzListItemComponent } from './nz-list-item.component';
import { NzListComponent } from './nz-list.component';
var NzListModule = /** @class */ (function () {
    function NzListModule() {
    }
    NzListModule.decorators = [
        { type: NgModule, args: [{
                    imports: [CommonModule, NzSpinModule, NzGridModule, NzAvatarModule, NzAddOnModule, NzEmptyModule],
                    declarations: [NzListComponent, NzListItemComponent, NzListItemMetaComponent],
                    exports: [NzListComponent, NzListItemComponent, NzListItemMetaComponent]
                },] }
    ];
    return NzListModule;
}());
export { NzListModule };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotbGlzdC5tb2R1bGUuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsibGlzdC9uei1saXN0Lm1vZHVsZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLFlBQVksRUFBRSxNQUFNLGlCQUFpQixDQUFDO0FBQy9DLE9BQU8sRUFBRSxRQUFRLEVBQUUsTUFBTSxlQUFlLENBQUM7QUFFekMsT0FBTyxFQUFFLGNBQWMsRUFBRSxNQUFNLDRCQUE0QixDQUFDO0FBQzVELE9BQU8sRUFBRSxhQUFhLEVBQUUsTUFBTSw0QkFBNEIsQ0FBQztBQUMzRCxPQUFPLEVBQUUsYUFBYSxFQUFFLE1BQU0sMEJBQTBCLENBQUM7QUFDekQsT0FBTyxFQUFFLFlBQVksRUFBRSxNQUFNLHdCQUF3QixDQUFDO0FBQ3RELE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSx3QkFBd0IsQ0FBQztBQUV0RCxPQUFPLEVBQUUsdUJBQXVCLEVBQUUsTUFBTSwrQkFBK0IsQ0FBQztBQUN4RSxPQUFPLEVBQUUsbUJBQW1CLEVBQUUsTUFBTSwwQkFBMEIsQ0FBQztBQUMvRCxPQUFPLEVBQUUsZUFBZSxFQUFFLE1BQU0scUJBQXFCLENBQUM7QUFFdEQ7SUFBQTtJQUsyQixDQUFDOztnQkFMM0IsUUFBUSxTQUFDO29CQUNSLE9BQU8sRUFBRSxDQUFDLFlBQVksRUFBRSxZQUFZLEVBQUUsWUFBWSxFQUFFLGNBQWMsRUFBRSxhQUFhLEVBQUUsYUFBYSxDQUFDO29CQUNqRyxZQUFZLEVBQUUsQ0FBQyxlQUFlLEVBQUUsbUJBQW1CLEVBQUUsdUJBQXVCLENBQUM7b0JBQzdFLE9BQU8sRUFBRSxDQUFDLGVBQWUsRUFBRSxtQkFBbUIsRUFBRSx1QkFBdUIsQ0FBQztpQkFDekU7O0lBQzBCLG1CQUFDO0NBQUEsQUFMNUIsSUFLNEI7U0FBZixZQUFZIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgQ29tbW9uTW9kdWxlIH0gZnJvbSAnQGFuZ3VsYXIvY29tbW9uJztcbmltcG9ydCB7IE5nTW9kdWxlIH0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5cbmltcG9ydCB7IE56QXZhdGFyTW9kdWxlIH0gZnJvbSAnLi4vYXZhdGFyL256LWF2YXRhci5tb2R1bGUnO1xuaW1wb3J0IHsgTnpBZGRPbk1vZHVsZSB9IGZyb20gJy4uL2NvcmUvYWRkb24vYWRkb24ubW9kdWxlJztcbmltcG9ydCB7IE56RW1wdHlNb2R1bGUgfSBmcm9tICcuLi9lbXB0eS9uei1lbXB0eS5tb2R1bGUnO1xuaW1wb3J0IHsgTnpHcmlkTW9kdWxlIH0gZnJvbSAnLi4vZ3JpZC9uei1ncmlkLm1vZHVsZSc7XG5pbXBvcnQgeyBOelNwaW5Nb2R1bGUgfSBmcm9tICcuLi9zcGluL256LXNwaW4ubW9kdWxlJztcblxuaW1wb3J0IHsgTnpMaXN0SXRlbU1ldGFDb21wb25lbnQgfSBmcm9tICcuL256LWxpc3QtaXRlbS1tZXRhLmNvbXBvbmVudCc7XG5pbXBvcnQgeyBOekxpc3RJdGVtQ29tcG9uZW50IH0gZnJvbSAnLi9uei1saXN0LWl0ZW0uY29tcG9uZW50JztcbmltcG9ydCB7IE56TGlzdENvbXBvbmVudCB9IGZyb20gJy4vbnotbGlzdC5jb21wb25lbnQnO1xuXG5ATmdNb2R1bGUoe1xuICBpbXBvcnRzOiBbQ29tbW9uTW9kdWxlLCBOelNwaW5Nb2R1bGUsIE56R3JpZE1vZHVsZSwgTnpBdmF0YXJNb2R1bGUsIE56QWRkT25Nb2R1bGUsIE56RW1wdHlNb2R1bGVdLFxuICBkZWNsYXJhdGlvbnM6IFtOekxpc3RDb21wb25lbnQsIE56TGlzdEl0ZW1Db21wb25lbnQsIE56TGlzdEl0ZW1NZXRhQ29tcG9uZW50XSxcbiAgZXhwb3J0czogW056TGlzdENvbXBvbmVudCwgTnpMaXN0SXRlbUNvbXBvbmVudCwgTnpMaXN0SXRlbU1ldGFDb21wb25lbnRdXG59KVxuZXhwb3J0IGNsYXNzIE56TGlzdE1vZHVsZSB7fVxuIl19